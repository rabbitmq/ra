%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%% @hidden
-module(ra_log_segment).

-export([open/1,
         open/2,
         append/4,
         sync/1,
         read/3,
         read_cons/5,
         term_query/2,
         close/1,
         range/1,
         flush/1,
         max_count/1,
         filename/1,
         segref/1,
         is_same_as/2]).

-export([dump/1,
         dump_index/1]).

-include("ra.hrl").

-define(VERSION, 2).
-define(MAGIC, "RASG").
-define(HEADER_SIZE, 4 + (16 div 8) + (16 div 8)).
-define(DEFAULT_INDEX_MAX_COUNT, 4096).
-define(DEFAULT_MAX_PENDING, 1024).
-define(INDEX_RECORD_SIZE_V1, ((2 * 64 + 3 * 32) div 8)).
-define(INDEX_RECORD_SIZE_V2, ((3 * 64 + 2 * 32) div 8)).
-define(READ_AHEAD_B, 64000).

-type index_record_data() :: {Term :: ra_term(), % 64 bit
                              Offset :: non_neg_integer(), % 32 bit
                              Length :: non_neg_integer(), % 32 bit
                              Checksum :: integer()}. % CRC32 - 32 bit

-type ra_segment_index() :: #{ra_index() => index_record_data()}.

-record(cfg, {version :: non_neg_integer(),
              max_count = ?DEFAULT_INDEX_MAX_COUNT :: non_neg_integer(),
              max_pending = ?DEFAULT_MAX_PENDING :: non_neg_integer(),
              filename :: file:filename_all(),
              fd :: maybe(file:io_device()),
              index_size :: pos_integer(),
              access_pattern :: sequential | random,
              mode = append :: read | append}).

-record(state,
        {cfg :: #cfg{},
         index_offset :: pos_integer(),
         index_write_offset :: pos_integer(),
         data_start :: pos_integer(),
         data_offset :: pos_integer(),
         data_write_offset :: pos_integer(),
         index = undefined :: maybe(ra_segment_index()),
         range :: maybe({ra_index(), ra_index()}),
         pending_data = [] :: iodata(),
         pending_index = [] :: iodata(),
         pending_count = 0 :: non_neg_integer(),
         cache :: undefined | {non_neg_integer(), non_neg_integer(), binary()}
        }).

-type ra_log_segment_options() :: #{max_count => non_neg_integer(),
                                    max_pending => non_neg_integer(),
                                    mode => append | read,
                                    access_pattern => sequential | random}.
-opaque state() :: #state{}.

-export_type([state/0,
              ra_log_segment_options/0]).

-spec open(Filename :: file:filename_all()) ->
    {ok, state()} | {error, term()}.
open(Filename) ->
    open(Filename, #{}).

-spec open(Filename :: file:filename_all(),
           Options :: ra_log_segment_options()) ->
    {ok, state()} | {error, term()}.
open(Filename, Options) ->
    Mode = maps:get(mode, Options, append),
    FileExists = case Mode of
                     append ->
                         case prim_file:read_file_info(Filename) of
                             {ok, _} -> true;
                             _ ->
                                 false
                         end;
                     read ->
                         %% assume file exists
                         %% it will fail to open later if it does not
                         true
                 end,
    Modes = case Mode of
                append ->
                    [read, write, raw, binary];
                read ->
                    [read, raw, binary]
            end,
    case ra_file_handle:open(Filename, Modes) of
        {ok, Fd} ->
            process_file(FileExists, Mode, Filename, Fd, Options);
        Err -> Err
    end.

process_file(true, Mode, Filename, Fd, Options) ->
    case read_header(Fd) of
        {ok, Version, MaxCount} ->
            MaxPending = maps:get(max_pending, Options, ?DEFAULT_MAX_PENDING),
            IndexRecordSize = index_record_size(Version),
            IndexSize = MaxCount * IndexRecordSize,
            {NumIndexRecords, DataOffset, Range, Index} =
                recover_index(Fd, Version, MaxCount),
            IndexOffset = ?HEADER_SIZE + NumIndexRecords * IndexRecordSize,
            AccessPattern = maps:get(access_pattern, Options, random),
            Mode = maps:get(mode, Options, append),
            {ok, #state{cfg = #cfg{version = Version,
                                   max_count = MaxCount,
                                   max_pending = MaxPending,
                                   filename = Filename,
                                   mode = Mode,
                                   index_size = IndexSize,
                                   access_pattern = AccessPattern,
                                   fd = Fd},
                    data_start = ?HEADER_SIZE + IndexSize,
                    data_offset = DataOffset,
                    data_write_offset = DataOffset,
                    index_offset = IndexOffset,
                    index_write_offset = IndexOffset,
                    range = Range,
                    cache = undefined,
                    % we don't need an index in memory in append mode
                    index = case Mode of
                                read -> Index;
                                append -> undefined
                            end}};
        Err ->
            Err
    end;
process_file(false, Mode, Filename, Fd, Options) ->
    MaxCount = maps:get(max_count, Options, ?DEFAULT_INDEX_MAX_COUNT),
    MaxPending = maps:get(max_pending, Options, ?DEFAULT_MAX_PENDING),
    IndexSize = MaxCount * ?INDEX_RECORD_SIZE_V2,
    ok = write_header(MaxCount, Fd),
    {ok, #state{cfg = #cfg{version = ?VERSION,
                           max_count = MaxCount,
                           max_pending = MaxPending,
                           filename = Filename,
                           mode = Mode,
                           index_size = IndexSize,
                           fd = Fd,
                           access_pattern = random},
                index_write_offset = ?HEADER_SIZE,
                index_offset = ?HEADER_SIZE,
                data_start = ?HEADER_SIZE + IndexSize,
                data_offset = ?HEADER_SIZE + IndexSize,
                data_write_offset = ?HEADER_SIZE + IndexSize
               }}.

-spec append(state(), ra_index(), ra_term(), binary()) ->
    {ok, state()} | {error, full | inet:posix()}.
append(#state{cfg = #cfg{max_pending = PendingCount},
              pending_count = PendingCount} = State0,
       Index, Term, Data) ->
    case flush(State0) of
        {ok, State} ->
            append(State, Index, Term, Data);
        Err ->
            Err
    end;
append(#state{cfg = #cfg{version = Version,
                         mode = append},
              index_offset = IndexOffset,
              data_start = DataStart,
              data_offset = DataOffset,
              range = Range0,
              pending_count = PendCnt,
              pending_index = IdxPend0,
              pending_data = DataPend0} = State,
       Index, Term, Data) ->
    % check if file is full
    case IndexOffset < DataStart of
        true ->
            Length = erlang:byte_size(Data),
            % TODO: check length is less than #FFFFFFFF ??
            Checksum = erlang:crc32(Data),
            OSize = offset_size(Version),
            IndexData = <<Index:64/unsigned, Term:64/unsigned,
                          DataOffset:OSize/unsigned, Length:32/unsigned,
                          Checksum:32/unsigned>>,
            Range = update_range(Range0, Index),
            % fsync is done explicitly
            {ok, State#state{index_offset = IndexOffset + index_record_size(Version),
                             data_offset = DataOffset + Length,
                             range = Range,
                             pending_index = [IdxPend0, IndexData],
                             pending_data = [DataPend0, Data],
                             pending_count = PendCnt + 1}
            };
        false ->
            {error, full}
     end.

-spec sync(state()) -> {ok, state()} | {error, term()}.
sync(#state{cfg = #cfg{fd = Fd},
            pending_index = []} = State) ->
    case ra_file_handle:sync(Fd) of
        ok ->
            {ok, State};
        {error, _} = Err ->
            Err
    end;
sync(State0) ->
    case flush(State0) of
        {ok, State} ->
            sync(State);
        Err ->
            Err
    end.

-spec flush(state()) -> {ok, state()} | {error, term()}.
flush(#state{cfg = #cfg{fd = Fd},
             pending_data = PendData,
             pending_index = PendIndex,
             index_offset = IdxOffs,
             data_offset = DataOffs,
             index_write_offset = IdxWriteOffs,
             data_write_offset = DataWriteOffs} = State) ->
    case ra_file_handle:pwrite(Fd, DataWriteOffs, PendData) of
        ok ->
            case ra_file_handle:pwrite(Fd, IdxWriteOffs, PendIndex) of
                ok ->
                    {ok, State#state{pending_data = [],
                                     pending_index = [],
                                     pending_count = 0,
                                     index_write_offset = IdxOffs,
                                     data_write_offset = DataOffs}};
                Err ->
                    Err
            end;
        Err ->
            Err
    end.

-spec read(state(), Idx :: ra_index(), Num :: non_neg_integer()) ->
    [{ra_index(), ra_term(), binary()}].
read(State, Idx, Num) ->
    read_cons(State, Idx, Num, fun ra_lib:id/1, []).


-spec read_cons(state(), ra_index(), Num :: non_neg_integer(),
                fun((binary()) -> term()), Acc) ->
    Acc when Acc :: [{ra_index(), ra_term(), binary()}].
read_cons(#state{cfg = #cfg{fd = _Fd, mode = read},
                 index = Index} = State, Idx,
          Num, Fun, Acc) ->
    pread_cons(State, Idx, Idx + Num - 1, Index, Fun, Acc).

-spec term_query(state(), Idx :: ra_index()) -> maybe(ra_term()).
term_query(#state{index = Index}, Idx) ->
    case Index of
        #{Idx := {Term, _, _, _}} ->
            Term;
        _ -> undefined
    end.

pread_cons(_Fd, Idx, FinalIdx, _, _Fun, Acc)
  when Idx > FinalIdx ->
    Acc;
pread_cons(#state{cfg = #cfg{fd = _Fd}} = State0, Idx,
           FinalIdx, Index, Fun, Acc) ->
    case Index of
        #{Idx := {Term, Offset, Length, Crc} = IdxRec} ->
            case pread(State0, Offset, Length) of
                {ok, Data, State} ->
                    %% performc crc check
                    case erlang:crc32(Data) of
                        Crc ->
                            [{Idx, Term, Fun(Data)} |
                             pread_cons(State, Idx+1, FinalIdx, Index, Fun, Acc)];
                        _ ->
                            %% CRC check failures are irrecoverable
                            exit({ra_log_segment_crc_check_failure, Idx, IdxRec,
                                  State#state.cfg#cfg.filename})
                    end;
                {error, partial_data} ->
                    %% we did not read the correct number of bytes suggesting
                    exit({ra_log_segment_unexpected_eof, Idx, IdxRec,
                          State0#state.cfg#cfg.filename})
            end;
        _ ->
            pread_cons(State0, Idx+1, FinalIdx, Index, Fun, Acc)
    end.

-spec range(state()) -> maybe({ra_index(), ra_index()}).
range(#state{range = Range}) ->
    Range.

-spec max_count(state()) -> non_neg_integer().
max_count(#state{cfg = #cfg{max_count = Max}}) ->
    Max.

-spec filename(state()) -> file:filename().
filename(#state{cfg = #cfg{filename = Fn}}) ->
    Fn.

-spec segref(state()) -> maybe(ra_log:segment_ref()).
segref(#state{range = undefined}) ->
    undefined;
segref(#state{range = {Start, End},
              cfg = #cfg{filename = Fn}}) ->
    {Start, End, ra_lib:to_string(filename:basename(Fn))}.

-spec is_same_as(state(), file:filename_all()) -> boolean().
is_same_as(#state{cfg = #cfg{filename = Fn0}}, Fn) ->
    is_same_filename_all(Fn0, Fn).

-spec close(state()) -> ok.
close(#state{cfg = #cfg{fd = Fd, mode = append}} = State) ->
    % close needs to be defensive and idempotent so we ignore the return
    % values here
    _ = sync(State),
    _ = ra_file_handle:close(Fd),
    ok;
close(#state{cfg = #cfg{fd = Fd}}) ->
    _ = ra_file_handle:close(Fd),
    ok.

%%% Internal

is_same_filename_all(Fn, Fn) ->
    true;
is_same_filename_all(Fn0, Fn1) ->
    B0 = filename:basename(Fn0),
    B1 = filename:basename(Fn1),
    ra_lib:to_list(B0) == ra_lib:to_list(B1).

update_range(undefined, Idx) ->
    {Idx, Idx};
update_range({First, _Last}, Idx) ->
    {min(First, Idx), Idx}.

recover_index(Fd, Version, MaxCount) ->
    IndexSize = MaxCount * index_record_size(Version),
    DataOffset = ?HEADER_SIZE + IndexSize,
    case ra_file_handle:pread(Fd, ?HEADER_SIZE, IndexSize) of
        {ok, Data} ->
            parse_index_data(Version, Data, DataOffset);
        eof ->
            % if no entries have been written the file hasn't "stretched"
            % to where the data offset starts.
            {0, DataOffset, undefined, #{}}
    end.

dump_index(File) ->
    {ok, Fd} = file:open(File, [read, raw, binary
                               ]),
    {ok, Version, MaxCount} = read_header(Fd),
    IndexSize = MaxCount * index_record_size(Version),
    {ok, ?HEADER_SIZE} = file:position(Fd, ?HEADER_SIZE),
    DataOffset = ?HEADER_SIZE + IndexSize,
    case file:read(Fd, IndexSize) of
        {ok, Data} ->
            D = [begin
                     % {ok, _} = file:position(Fd, O),
                     % {ok, B} = file:read(Fd, N),
                     {I, T, O}
                 end || {I, T, O, _N} <- dump_index_data(Data, [])],
            _ = file:close(Fd),
            D;
        eof ->
            _ = file:close(Fd),
            % if no entries have been written the file hasn't "stretched"
            % to where the data offset starts.
            {0, DataOffset, undefined, #{}}
    end.

dump(File) ->
    {ok, S0} = open(File, #{mode => read}),
    {Idx, Last} = range(S0),
    L = read_cons(S0, Idx, Last - Idx + 1, fun erlang:binary_to_term/1, []),
    close(S0),
    L.


dump_index_data(<<Idx:64/unsigned, Term:64/unsigned,
                  Offset:64/unsigned, Length:32/unsigned,
                  _:32/unsigned, Rest/binary>>,
                 Acc) ->
dump_index_data(Rest, [{Idx, Term, Offset, Length} | Acc]);
dump_index_data(_, Acc) ->
    lists:reverse(Acc).

parse_index_data(2, Data, DataOffset) ->
    parse_index_data(Data, 0, 0, DataOffset, undefined, #{});
parse_index_data(1, Data, DataOffset) ->
    parse_index_data_v1(Data, 0, 0, DataOffset, undefined, #{}).

parse_index_data(<<>>, Num, _LastIdx, DataOffset, Range, Index) ->
    % end of data
    {Num, DataOffset, Range, Index};
parse_index_data(<<0:64/unsigned, 0:64/unsigned, 0:64/unsigned,
                   0:32/unsigned, 0:32/integer, _Rest/binary>>,
                 Num, _LastIdx, DataOffset, Range, Index) ->
    % partially written index
    % end of written data
    {Num, DataOffset, Range, Index};
parse_index_data(<<Idx:64/unsigned, Term:64/unsigned,
                   Offset:64/unsigned, Length:32/unsigned,
                   Crc:32/integer, Rest/binary>>,
                 Num, LastIdx, _DataOffset, Range, Index0) ->
    % trim index entries if Idx goes "backwards"
    Index = case Idx < LastIdx of
                true -> maps:filter(fun (K, _) when K > Idx -> false;
                                        (_, _) -> true
                                    end, Index0);
                false -> Index0
            end,
    parse_index_data(Rest, Num+1, Idx,
                     Offset + Length,
                     update_range(Range, Idx),
                     Index#{Idx => {Term, Offset, Length, Crc}}).

parse_index_data_v1(<<>>, Num, _LastIdx, DataOffset, Range, Index) ->
    % end of data
    {Num, DataOffset, Range, Index};
parse_index_data_v1(<<0:64/unsigned, 0:64/unsigned, 0:32/unsigned,
                   0:32/unsigned, 0:32/integer, _Rest/binary>>,
                 Num, _LastIdx, DataOffset, Range, Index) ->
    % partially written index
    % end of written data
    {Num, DataOffset, Range, Index};
parse_index_data_v1(<<Idx:64/unsigned, Term:64/unsigned,
                   Offset:32/unsigned, Length:32/unsigned,
                   Crc:32/integer, Rest/binary>>,
                 Num, LastIdx, _DataOffset, Range, Index0) ->
    % trim index entries if Idx goes "backwards"
    Index = case Idx < LastIdx of
                true -> maps:filter(fun (K, _) when K > Idx -> false;
                                        (_, _) -> true
                                    end, Index0);
                false -> Index0
            end,
    parse_index_data_v1(Rest, Num+1, Idx,
                     Offset + Length,
                     update_range(Range, Idx),
                     Index#{Idx => {Term, Offset, Length, Crc}}).

write_header(MaxCount, Fd) ->
    Header = <<?MAGIC, ?VERSION:16/unsigned, MaxCount:16/unsigned>>,
    {ok, 0} = ra_file_handle:position(Fd, 0),
    ok = ra_file_handle:write(Fd, Header),
    ok = ra_file_handle:sync(Fd).

read_header(Fd) ->
    case ra_file_handle:pread(Fd, 0, ?HEADER_SIZE) of
        {ok, Buffer} ->
            case Buffer of
                <<?MAGIC, Version:16/unsigned, MaxCount:16/unsigned>>
                  when Version =< ?VERSION ->
                    {ok, Version, MaxCount};
                _ ->
                    {error, invalid_segment_format}
            end;
        eof ->
            {error, missing_segment_header};
        {error, _} = Err ->
            Err
    end.

pread(#state{cfg = #cfg{access_pattern = random,
                        fd = Fd}} = State, Pos, Length) ->
    %% no cache
    {ok, Data} = ra_file_handle:pread(Fd, Pos, Length),
    case byte_size(Data)  of
        Length ->
            {ok, Data, State};
        _ ->
            {error, partial_data}
    end;
pread(#state{cfg = #cfg{fd = _Fd},
             cache = {CPos, CLen, Bin}} = State, Pos, Length)
  when Pos >= CPos andalso
       Pos + Length =< (CPos + CLen) ->
    %% read fits inside cache
    {ok, binary:part(Bin, Pos - CPos, Length), State};
pread(#state{cfg = #cfg{fd = Fd},
             cache = undefined} = State, Pos, Length) ->
    CacheLen = max(Length, ?READ_AHEAD_B),
    {ok, Cache} = ra_file_handle:pread(Fd, Pos, CacheLen),
    case byte_size(Cache) >= Length  of
        true ->
            pread(State#state{cache = {Pos, byte_size(Cache), Cache}},
                  Pos, Length);
        false ->
            {error, partial_data}
    end;
pread(#state{cache = {_, _, _}} = State, Pos, Length) ->
    %% invalidate cache
    pread(State#state{cache = undefined}, Pos, Length).


offset_size(2) -> 64;
offset_size(1) -> 32.

index_record_size(2) ->
    ?INDEX_RECORD_SIZE_V2;
index_record_size(1) ->
    ?INDEX_RECORD_SIZE_V1.
