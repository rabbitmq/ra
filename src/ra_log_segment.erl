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
         max_count/1,
         filename/1,
         segref/1,
         is_same_as/2]).

-export([dump_index/1]).

-include("ra.hrl").

-define(VERSION, 1).
-define(HEADER_SIZE, (16 div 8) + (16 div 8)). % {
-define(DEFAULT_INDEX_MAX_COUNT, 4096).
-define(INDEX_RECORD_SIZE, ((2 * 64 + 3 * 32) div 8)).

-type index_record_data() :: {Term :: ra_term(), % 64 bit
                              Offset :: non_neg_integer(), % 32 bit
                              Length :: non_neg_integer(), % 32 bit
                              Checksum :: integer()}. % CRC32 - 32 bit

-type ra_segment_index() :: #{ra_index() => index_record_data()}.

-record(state,
        {version :: non_neg_integer(),
         max_count = ?DEFAULT_INDEX_MAX_COUNT :: non_neg_integer(),
         filename :: file:filename_all(),
         fd :: maybe(file:io_device()),
         index_size :: pos_integer(),
         index_offset :: pos_integer(),
         data_start :: pos_integer(),
         data_offset :: pos_integer(),
         mode = append :: read | append,
         index = undefined :: maybe(ra_segment_index()),
         range :: maybe({ra_index(), ra_index()}),
         pending = [] :: [{non_neg_integer(), binary()}]
        }).

-type ra_log_segment_options() :: #{max_count => non_neg_integer(),
                                         mode => append | read}.
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
    AbsFilename = filename:absname(Filename),
    FileExists = filelib:is_file(AbsFilename),
    Mode = maps:get(mode, Options, append),
    Modes = case Mode of
                append -> [read, write, raw, binary];
                read -> [read, raw, read_ahead, binary]
            end,
    case ra_file_handle:open(AbsFilename, Modes) of
        {ok, Fd} ->
            process_file(FileExists, Mode, Filename, Fd, Options);
        Err -> Err
    end.

process_file(true, Mode, Filename, Fd, _Options) ->
    case read_header(Fd) of
        {ok, MaxCount} ->
            IndexSize = MaxCount * ?INDEX_RECORD_SIZE,
            {NumIndexRecords, DataOffset, Range, Index} =
                recover_index(Fd, MaxCount),
            IndexOffset = ?HEADER_SIZE + NumIndexRecords * ?INDEX_RECORD_SIZE,
            {ok, #state{version = 1,
                        max_count = MaxCount,
                        filename = Filename,
                        fd = Fd,
                        index_size = IndexSize,
                        mode = Mode,
                        data_start = ?HEADER_SIZE + IndexSize,
                        data_offset = DataOffset,
                        index_offset = IndexOffset,
                        range = Range,
                        % TODO: we don't need an index in memory in append mode
                        index = Index}};
        Err ->
            Err
    end;
process_file(false, Mode, Filename, Fd, Options) ->
    MaxCount = maps:get(max_count, Options, ?DEFAULT_INDEX_MAX_COUNT),
    IndexSize = MaxCount * ?INDEX_RECORD_SIZE,
    ok = write_header(MaxCount, Fd),
    {ok, #state{version = 1,
                max_count = MaxCount,
                filename = Filename,
                fd = Fd,
                index_size = IndexSize,
                index_offset = ?HEADER_SIZE,
                mode = Mode,
                data_start = ?HEADER_SIZE + IndexSize,
                data_offset = ?HEADER_SIZE + IndexSize}}.

-spec append(state(), ra_index(), ra_term(), binary()) ->
    {ok, state()} | {error, full}.
append(#state{index_offset = IndexOffset,
              data_start = DataStart,
              data_offset = DataOffset,
              range = Range0,
              mode = append,
              pending = Pend0} = State,
       Index, Term, Data) ->
    % check if file is full
    case IndexOffset < DataStart of
        true ->
            Length = erlang:byte_size(Data),
            % TODO: check length is less than #FFFFFFFF ??
            Checksum = erlang:crc32(Data),
            IndexData = <<Index:64/integer, Term:64/integer,
                          DataOffset:32/integer, Length:32/integer,
                          Checksum:32/integer>>,
            Pend = [{DataOffset, Data}, {IndexOffset, IndexData} | Pend0],
            Range = update_range(Range0, Index),
            % fsync is done explicitly
            {ok, State#state{index_offset = IndexOffset + ?INDEX_RECORD_SIZE,
                             data_offset = DataOffset + Length,
                             range = Range,
                             pending = Pend}};
        false ->
            {error, full}
     end.

-spec sync(state()) -> {ok, state()} | {error, term()}.
sync(#state{fd = Fd, pending = []} = State) ->
    case ra_file_handle:sync(Fd) of
        ok ->
            {ok, State};
        {error, _} = Err ->
            Err
    end;
sync(#state{fd = Fd, pending = Pend} = State) ->
    case ra_file_handle:pwrite(Fd, Pend) of
        ok ->
            sync(State#state{pending = []});
        {error, _} = Err ->
            Err
    end.

-spec read(state(), Idx :: ra_index(), Num :: non_neg_integer()) ->
    [{ra_index(), ra_term(), binary()}].
read(State, Idx, Num) ->
    % TODO: should we better indicate when records aren't found?
    % This depends on the semantics we want from a segment
    read_cons(State, Idx, Num, fun ra_lib:id/1, []).


-spec read_cons(state(), ra_index(), Num :: non_neg_integer(),
                fun((binary()) -> term()), Acc) ->
    Acc when Acc :: [{ra_index(), ra_term(), binary()}].
read_cons(#state{fd = Fd, mode = read, index = Index}, Idx,
          Num, Fun, Acc) ->
    % TODO: should we better indicate when records aren't found?
    % This depends on the semantics we want from a segment
    {Locs, Metas} = read_locs(Idx, Idx + Num, Index, {[], []}),
    {ok, Datas} = ra_file_handle:pread(Fd, Locs),
    combine_with(Metas, Datas, Fun, Acc).

-spec term_query(state(), Idx :: ra_index()) ->
    maybe(ra_term()).
term_query(#state{index = Index}, Idx) ->
    case Index of
        #{Idx := {Term, _, _, _}} ->
            Term;
        _ -> undefined
    end.

combine_with([], [], _, Acc) ->
    Acc;
combine_with([{_, _, Crc} = Meta | MetaTail],
             [Data | DataTail], Fun, Acc) ->
    % TODO: make checksum check optional
    case erlang:crc32(Data) of
        Crc -> ok;
        _ ->
            exit(ra_log_segment_crc_check_failure)
    end,
    combine_with(MetaTail, DataTail, Fun,
                 [setelement(3, Meta, Fun(Data)) | Acc]).

read_locs(Idx, Idx, _, Acc) ->
    Acc;
read_locs(Idx, FinalIdx, Index, {Locs, Meta} = Acc) ->
    case Index of
        #{Idx := {Term, Offset, Length, Crc}} ->
            read_locs(Idx+1, FinalIdx, Index,
                      {[{Offset, Length} | Locs], [{Idx, Term, Crc} | Meta]});
        _ ->
            read_locs(Idx+1, FinalIdx, Index, Acc)
    end.

-spec range(state()) -> maybe({ra_index(), ra_index()}).
range(#state{range = Range}) ->
    Range.

-spec max_count(state()) -> non_neg_integer().
max_count(#state{max_count = Max}) ->
    Max.

-spec filename(state()) -> file:filename().
filename(#state{filename = Fn}) ->
    filename:absname(Fn).

-spec segref(state()) -> maybe(ra_log:segment_ref()).
segref(#state{range = undefined}) ->
    undefined;
segref(#state{range = {Start, End}, filename = Fn}) ->
    {Start, End, ra_lib:to_string(filename:basename(Fn))}.

-spec is_same_as(state(), file:filename_all()) -> boolean().
is_same_as(#state{filename = Fn0}, Fn) ->
    is_same_filename_all(Fn0, Fn).

-spec close(state()) -> ok.
close(#state{fd = Fd, mode = append} = State) ->
    % close needs to be defensive and idempotent so we ignore the return
    % values here
    _ = sync(State),
    _ = ra_file_handle:close(Fd),
    ok;
close(#state{fd = Fd}) ->
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

recover_index(Fd, MaxCount) ->
    IndexSize = MaxCount * ?INDEX_RECORD_SIZE,
    {ok, ?HEADER_SIZE} = ra_file_handle:position(Fd, ?HEADER_SIZE),
    DataOffset = ?HEADER_SIZE + IndexSize,
    case ra_file_handle:read(Fd, IndexSize) of
        {ok, Data} ->
            parse_index_data(Data, DataOffset);
        eof ->
            % if no entries have been written the file hasn't "stretched"
            % to where the data offset starts.
            {0, DataOffset, undefined, #{}}
    end.

dump_index(File) ->
    {ok, Fd} = file:open(File, [read, raw, binary
                               ]),
    {ok, MaxCount} = read_header(Fd),
    IndexSize = MaxCount * ?INDEX_RECORD_SIZE,
    {ok, ?HEADER_SIZE} = file:position(Fd, ?HEADER_SIZE),
    DataOffset = ?HEADER_SIZE + IndexSize,
    case file:read(Fd, IndexSize) of
        {ok, Data} ->
            D = [begin
                     file:position(Fd, O),
                     {ok, B} = file:read(Fd, N),
                     {I, T, binary_to_term(B)}
                 end || {I, T, O, N} <- dump_index_data(Data, [])],
            file:close(Fd),
            D;
        eof ->
            file:close(Fd),
            % if no entries have been written the file hasn't "stretched"
            % to where the data offset starts.
            {0, DataOffset, undefined, #{}}
    end.

dump_index_data(<<Idx:64/integer, Term:64/integer,
                  Offset:32/integer, Length:32/integer,
                  _:32/integer, Rest/binary>>,
                 Acc) ->
dump_index_data(Rest, [{Idx, Term, Offset, Length} | Acc]);
dump_index_data(_, Acc) ->
    lists:reverse(Acc).

parse_index_data(Data, DataOffset) ->
    parse_index_data(Data, 0, 0, DataOffset, undefined, #{}).

parse_index_data(<<>>, Num, _LastIdx, DataOffset, Range, Index) ->
    % end of data
    {Num, DataOffset, Range, Index};
parse_index_data(<<0:64/integer, 0:64/integer, 0:32/integer,
                   0:32/integer, 0:32/integer, _Rest/binary>>,
                 Num, _LastIdx, DataOffset, Range, Index) ->
    % partially written index
    % end of written data
    {Num, DataOffset, Range, Index};
parse_index_data(<<Idx:64/integer, Term:64/integer,
                   Offset:32/integer, Length:32/integer,
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

write_header(MaxCount, Fd) ->
    Header = <<?VERSION:16/integer, MaxCount:16/integer>>,
    {ok, 0} = ra_file_handle:position(Fd, 0),
    ok = ra_file_handle:write(Fd, Header).

read_header(Fd) ->
    {ok, 0} = ra_file_handle:position(Fd, 0),
    case ra_file_handle:read(Fd, ?HEADER_SIZE) of
        {ok, Buffer} ->
            case Buffer of
                <<1:16/integer, MaxCount:16/integer>> ->
                    {ok, MaxCount};
                _ ->
                    {error, invalid_segment_version}
            end;
        eof ->
            {error, missing_segment_header};
        {error, _} = Err ->
            Err
    end.
