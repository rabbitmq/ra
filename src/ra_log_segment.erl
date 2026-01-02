%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2025 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @hidden
-module(ra_log_segment).

-export([open/1,
         open/2,
         append/4,
         sync/1,
         fold/6,
         fold/7,
         is_modified/1,
         read_sparse/4,
         read_sparse_no_checks/4,
         term_query/2,
         close/1,
         range/1,
         flush/1,
         max_count/1,
         filename/1,
         segref/1,
         info/1,
         info/2,
         is_same_as/2,
         copy/3]).

-export([dump/1,
         dump_index/1]).

-include("ra.hrl").

-include_lib("stdlib/include/assert.hrl").
-include_lib("kernel/include/file.hrl").

-define(VERSION, 2).
-define(MAGIC, "RASG").
-define(HEADER_SIZE, 4 + (16 div 8) + (16 div 8)).
-define(INDEX_RECORD_SIZE_V1, ((2 * 64 + 3 * 32) div 8)).
-define(INDEX_RECORD_SIZE_V2, ((3 * 64 + 2 * 32) div 8)).
-define(BLOCK_SIZE, 4096). %% assumed block size
-define(READ_AHEAD_B, ?BLOCK_SIZE * 16). %% some multiple of common block sizes

-type index_record_data() :: {Term :: ra_term(), % 64 bit
                              Offset :: non_neg_integer(), % 32 bit
                              Length :: non_neg_integer(), % 32 bit
                              Checksum :: integer()}. % CRC32 - 32 bit

-type ra_segment_index() :: #{ra_index() => index_record_data()}.

%% Index mode for read operations:
%% - map: Parse full index into Erlang map on open (default, best for many reads)
%% - binary: Keep raw index binary, binary search on demand (best for sparse reads)
-type index_mode() :: map | binary.

-record(cfg, {version :: non_neg_integer(),
              max_count = ?SEGMENT_MAX_ENTRIES :: non_neg_integer(),
              max_pending = ?SEGMENT_MAX_PENDING :: non_neg_integer(),
              max_size = ?SEGMENT_MAX_SIZE_B :: non_neg_integer(),
              filename :: file:filename_all(),
              fd :: option(file:io_device()),
              index_size :: pos_integer(),
              index_record_size :: pos_integer(),
              access_pattern :: sequential | random,
              file_advise = normal :: posix_file_advise(),
              mode = append :: read | append,
              index_mode = map :: index_mode(),
              compute_checksums = true :: boolean()}).

-record(state,
        {cfg :: #cfg{},
         index_offset :: pos_integer(),
         index_write_offset :: pos_integer(),
         data_start :: pos_integer(),
         data_offset :: pos_integer(),
         data_write_offset :: pos_integer(),
         %% For map mode: parsed index map
         %% For binary mode: raw index binary for binary search
         index = undefined :: option(ra_segment_index()) | binary(),
         num_entries = 0 :: non_neg_integer(),
         range :: option({ra_index(), ra_index()}),
         pending_data = [] :: iodata(),
         pending_index = [] :: iodata(),
         pending_count = 0 :: non_neg_integer(),
         cache :: undefined | {non_neg_integer(), non_neg_integer(), binary()}
        }).

-type posix_file_advise() :: 'normal' | 'sequential' | 'random'
                           | 'no_reuse' | 'will_need' | 'dont_need'.

-type ra_log_segment_options() :: #{max_count => non_neg_integer(),
                                    max_pending => non_neg_integer(),
                                    max_size => non_neg_integer(),
                                    compute_checksums => boolean(),
                                    mode => append | read,
                                    index_mode => index_mode(),
                                    access_pattern => sequential | random,
                                    file_advise => posix_file_advise()}.
-opaque state() :: #state{}.

-export_type([state/0,
              posix_file_advise/0,
              index_mode/0,
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
    case file:open(Filename, Modes) of
        {ok, Fd} ->
            process_file(FileExists, Mode, Filename, Fd, Options);
        Err -> Err
    end.

process_file(true, Mode, Filename, Fd, Options) ->
    AccessPattern = maps:get(access_pattern, Options, random),
    FileAdvise = maps:get(file_advise, Options, normal),
    IndexMode = maps:get(index_mode, Options, map),
    if FileAdvise == random andalso
       Mode == read ->
           Offs = maps:get(max_count, Options, ?SEGMENT_MAX_ENTRIES) * ?INDEX_RECORD_SIZE_V2,
           _ = file:advise(Fd, Offs, 0, random),
           ok;
       true ->
           ok
    end,
    case read_header(Fd) of
        {ok, Version, MaxCount} ->
            MaxPending = maps:get(max_pending, Options, ?SEGMENT_MAX_PENDING),
            MaxSize = maps:get(max_size, Options, ?SEGMENT_MAX_SIZE_B),
            IndexRecordSize = index_record_size(Version),
            IndexSize = MaxCount * IndexRecordSize,
            ComputeChecksums = maps:get(compute_checksums, Options, true),

            %% Choose index recovery strategy based on mode
            {NumIndexRecords, DataOffset, Range, Index, ActualIndexMode} =
                case {Mode, IndexMode} of
                    {read, binary} ->
                        %% Binary mode: keep raw index binary, don't parse into map
                        %% Falls back to map if overwrites detected
                        case recover_index_binary(Fd, Version, MaxCount) of
                            {ok, N, DO, R, IndexBin} ->
                                {N, DO, R, IndexBin, binary};
                            {fallback_to_map, N, DO, R, IndexMap} ->
                                %% Overwrites detected - use map mode instead
                                {N, DO, R, IndexMap, map}
                        end;
                    _ ->
                        %% Map mode (default) or append mode
                        {N, DO, R, I} = recover_index(Fd, Version, MaxCount),
                        {N, DO, R, I, map}
                end,
            IndexOffset = ?HEADER_SIZE + NumIndexRecords * IndexRecordSize,

            {ok, #state{cfg = #cfg{version = Version,
                                   max_count = MaxCount,
                                   max_pending = MaxPending,
                                   max_size = MaxSize,
                                   filename = Filename,
                                   mode = Mode,
                                   index_size = IndexSize,
                                   index_record_size = IndexRecordSize,
                                   access_pattern = AccessPattern,
                                   file_advise = FileAdvise,
                                   index_mode = ActualIndexMode,
                                   compute_checksums = ComputeChecksums,
                                   fd = Fd},
                    data_start = ?HEADER_SIZE + IndexSize,
                    data_offset = DataOffset,
                    data_write_offset = DataOffset,
                    index_offset = IndexOffset,
                    index_write_offset = IndexOffset,
                    range = Range,
                    num_entries = NumIndexRecords,
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
    MaxCount = maps:get(max_count, Options, ?SEGMENT_MAX_ENTRIES),
    MaxPending = maps:get(max_pending, Options, ?SEGMENT_MAX_PENDING),
    MaxSize = maps:get(max_size, Options, ?SEGMENT_MAX_SIZE_B),
    ComputeChecksums = maps:get(compute_checksums, Options, true),
    IndexSize = MaxCount * ?INDEX_RECORD_SIZE_V2,
    ok = write_header(MaxCount, Fd),
    FileAdvise = maps:get(file_advise, Options, dont_need),
    IndexMode = maps:get(index_mode, Options, map),
    {ok, #state{cfg = #cfg{version = ?VERSION,
                           max_count = MaxCount,
                           max_pending = MaxPending,
                           max_size = MaxSize,
                           filename = Filename,
                           mode = Mode,
                           index_size = IndexSize,
                           index_record_size = ?INDEX_RECORD_SIZE_V2,
                           fd = Fd,
                           compute_checksums = ComputeChecksums,
                           file_advise = FileAdvise,
                           index_mode = IndexMode,
                           access_pattern = random},
                index_write_offset = ?HEADER_SIZE,
                index_offset = ?HEADER_SIZE,
                data_start = ?HEADER_SIZE + IndexSize,
                data_offset = ?HEADER_SIZE + IndexSize,
                data_write_offset = ?HEADER_SIZE + IndexSize
               }}.

-spec append(state(), ra_index(), ra_term(),
             iodata() | {non_neg_integer(), iodata()}) ->
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
                         mode = append} = Cfg,
              index_offset = IndexOffset,
              data_offset = DataOffset,
              range = Range0,
              pending_count = PendCnt,
              pending_index = IdxPend0,
              pending_data = DataPend0} = State,
       Index, Term, {Length, Data}) ->

    case is_full(State) of
        false ->
            % TODO: check length is less than #FFFFFFFF ??
            Checksum = compute_checksum(Cfg, Data),
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
        true ->
            {error, full}
     end;
append(State, Index, Term, Data)
  when is_list(Data) orelse
       is_binary(Data) ->
    %% convert into {Size, Data} tuple
    append(State, Index, Term, {iolist_size(Data), Data}).

-spec sync(state()) -> {ok, state()} | {error, term()}.
sync(#state{cfg = #cfg{fd = Fd},
            pending_index = []} = State) ->
    case ra_file:sync(Fd) of
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
    case file:pwrite(Fd, DataWriteOffs, PendData) of
        ok ->
            case file:pwrite(Fd, IdxWriteOffs, PendIndex) of
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

-spec fold(state(),
           FromIdx :: ra_index(),
           ToIdx :: ra_index(),
           fun((binary()) -> term()),
           fun(({ra_index(), ra_term(), term()}, Acc) -> Acc), Acc) ->
    Acc when Acc :: term().
fold(#state{cfg = #cfg{mode = read} = Cfg,
            cache = Cache,
            index = Index,
            num_entries = NumEntries},
     FromIdx, ToIdx, Fun, AccFun, Acc) ->
    fold0(Cfg, Cache, FromIdx, ToIdx, Index, NumEntries, Fun, AccFun, Acc,
          error).

-spec fold(state(),
           FromIdx :: ra_index(),
           ToIdx :: ra_index(),
           fun((binary()) -> term()),
           fun(({ra_index(), ra_term(), term()}, Acc) -> Acc), Acc,
            MissingKeyStrat :: error | return) ->
    Acc when Acc :: term().
fold(#state{cfg = #cfg{mode = read} = Cfg,
            cache = Cache,
            index = Index,
            num_entries = NumEntries},
     FromIdx, ToIdx, Fun, AccFun, Acc, MissingKeyStrat) ->
    fold0(Cfg, Cache, FromIdx, ToIdx, Index, NumEntries, Fun, AccFun, Acc,
          MissingKeyStrat).

-spec is_modified(state()) -> boolean().
is_modified(#state{cfg = #cfg{fd = Fd},
                   data_offset = DataOffset} = State) ->
    case is_full(State) of
        true ->
            %% a full segment cannot be appended to.
            false;
        false ->
            %% get info and compare to data_offset
            {ok, #file_info{size = Size}} =
                prim_file:read_handle_info(Fd, [posix]),
            Size > DataOffset
    end.

-spec read_sparse(state(), [ra_index()],
                  fun((ra:index(), ra_term(), binary(), Acc) -> Acc),
                  Acc) ->
    {ok, NumRead :: non_neg_integer(), Acc} | {error, modified}
      when Acc :: term().
read_sparse(#state{} = State, Indexes, AccFun, Acc) ->
    case is_modified(State) of
        true ->
            {error, modified};
        false ->
            read_sparse_no_checks(State, Indexes, AccFun, Acc)
    end.

-spec read_sparse_no_checks(state(), [ra_index()],
                            fun((ra:index(), ra_term(), binary(), Acc) -> Acc),
                                Acc) ->
    {ok, NumRead :: non_neg_integer(), Acc}
      when Acc :: term().
read_sparse_no_checks(#state{index = Index,
                             num_entries = NumEntries,
                             cfg = #cfg{fd = Fd,
                                        version = Version,
                                        index_record_size = RecSize,
                                        index_mode = IndexMode}},
                      Indexes, AccFun, Acc) ->
    case IndexMode of
        binary when is_binary(Index) ->
            %% Binary mode: use binary search on the raw index binary
            %% Index is guaranteed monotonically increasing
            %% (overwrites fall back to map)
            read_sparse_binary(Fd, Version, RecSize, NumEntries, Index,
                               Indexes, AccFun, Acc, 0);
        _ ->
            %% Map mode: use map lookups with caching
            Cache0 = prepare_cache(Fd, Indexes, Index),
            read_sparse0(Fd, Indexes, Index, Cache0, Acc, AccFun, 0)
    end.

%% Binary mode: binary search on in-memory index binary
%% Uses a hint to optimize sequential ascending reads
%% Index is guaranteed monotonically increasing (overwrites fall back to map
%% mode)
read_sparse_binary(Fd, Version, RecSize, NumEntries, IndexBin,
                   Indexes, AccFun, Acc, Num) ->
    %% Prepare cache for consecutive runs (same optimization as map mode)
    Cache0 = prepare_cache_binary(Fd, Version, RecSize, NumEntries,
                                  IndexBin, Indexes),
    %% Start with no hint
    read_sparse_binary(Fd, Version, RecSize, NumEntries, IndexBin,
                       Indexes, AccFun, Acc, Num, undefined, Cache0).

read_sparse_binary(_Fd, _Version, _RecSize, _NumEntries, _IndexBin,
                   [], _AccFun, Acc, Num, _Hint, _Cache) ->
    {ok, Num, Acc};
read_sparse_binary(Fd, Version, RecSize, NumEntries, IndexBin,
                   [NextIdx | Rem] = Indexes, AccFun, Acc, Num, Hint, Cache0) ->
    case binary_search_index_hinted(Version, IndexBin, RecSize, NumEntries,
                                    NextIdx, Hint) of
        {ok, {Term, Pos, Length, _Crc}, FoundPos} ->
            case cache_read(Cache0, Pos, Length) of
                false ->
                    %% Cache miss - try to prepare a new cache or read directly
                    case prepare_cache_binary(Fd, Version, RecSize, NumEntries,
                                              IndexBin, Indexes) of
                        undefined ->
                            case file:pread(Fd, Pos, Length) of
                                {ok, Data} when byte_size(Data) == Length ->
                                    NewHint = {NextIdx, FoundPos},
                                    read_sparse_binary(Fd, Version, RecSize, NumEntries,
                                                       IndexBin, Rem, AccFun,
                                                       AccFun(NextIdx, Term, Data, Acc),
                                                       Num + 1, NewHint, undefined);
                                _ ->
                                    exit({read_error, NextIdx})
                            end;
                        Cache ->
                            %% Retry with new cache
                            read_sparse_binary(Fd, Version, RecSize, NumEntries,
                                               IndexBin, Indexes, AccFun, Acc,
                                               Num, Hint, Cache)
                    end;
                Data ->
                    %% Cache hit
                    NewHint = {NextIdx, FoundPos},
                    read_sparse_binary(Fd, Version, RecSize, NumEntries,
                                       IndexBin, Rem, AccFun,
                                       AccFun(NextIdx, Term, Data, Acc),
                                       Num + 1, NewHint, Cache0)
            end;
        not_found ->
            exit({missing_key, NextIdx})
    end.

%% Prepare cache for binary mode by finding consecutive index runs
prepare_cache_binary(_Fd, _Version, _RecSize, _NumEntries, _IndexBin, [_]) ->
    undefined;
prepare_cache_binary(Fd, Version, RecSize, NumEntries, IndexBin, 
                     [FirstIdx | Rem]) ->
    case consec_run(FirstIdx, FirstIdx, Rem) of
        {Idx, Idx} ->
            %% no run, no cache
            undefined;
        {FirstIdx, LastIdx} ->
            %% Found a consecutive run, look up positions via binary search
            %% Use hint from first lookup to speed up second lookup
            case binary_search_index_with_pos(Version, IndexBin, RecSize, 
                                              0, NumEntries - 1, FirstIdx) of
                {ok, {_, FstPos, FstLength, _}, FstFoundPos} ->
                    %% LastIdx > FirstIdx, so search forward from FstFoundPos
                    case binary_search_index_with_pos(Version, IndexBin, RecSize,
                                                      FstFoundPos, NumEntries - 1, 
                                                      LastIdx) of
                        {ok, {_, LastPos, LastLength, _}, _} ->
                            CacheLen = cache_length(FstPos, FstLength, 
                                                    LastPos, LastLength),
                            {ok, CacheData} = file:pread(Fd, FstPos, CacheLen),
                            {FstPos, byte_size(CacheData), CacheData};
                        not_found ->
                            undefined
                    end;
                not_found ->
                    undefined
            end
    end.

%% Binary search with hint optimization for sequential ascending reads
%% Index is guaranteed monotonically increasing (overwrites fall back to map mode)
%% If NextIdx > LastIdx, search forward from LastPos (common consumer pattern)
%% Returns {ok, Record, Position} or not_found
binary_search_index_hinted(Version, Bin, RecSize, NumEntries, Idx,
                           undefined) ->
    %% No hint, do full binary search
    binary_search_index_with_pos(Version, Bin, RecSize, 0, NumEntries - 1, Idx);
binary_search_index_hinted(Version, Bin, RecSize, NumEntries, Idx,
                           {LastIdx, LastPos})
  when Idx > LastIdx ->
    %% Ascending access pattern: search forward from last position
    %% First check if it's the very next entry (common case for sequential reads)
    NextPos = LastPos + 1,
    case NextPos < NumEntries of
        true ->
            case parse_index_entry(Version, Bin, NextPos * RecSize) of
                {ok, {Idx, Term, DataOffset, Length, Crc}} ->
                    %% Found it at the next position - O(1)!
                    {ok, {Term, DataOffset, Length, Crc}, NextPos};
                {ok, {FoundIdx, _, _, _, _}} when FoundIdx < Idx ->
                    %% Need to search forward from here (index is monotonic)
                    binary_search_index_with_pos(Version, Bin, RecSize,
                                                 NextPos + 1, NumEntries - 1,
                                                 Idx);
                _ ->
                    %% Index at NextPos is > Idx, search between LastPos and NextPos
                    binary_search_index_with_pos(Version, Bin, RecSize,
                                                 LastPos, NextPos, Idx)
            end;
        false ->
            not_found
    end;
binary_search_index_hinted(Version, Bin, RecSize, _NumEntries, Idx,
                           {LastIdx, LastPos})
  when Idx < LastIdx ->
    %% Descending access: search from 0 to LastPos
    binary_search_index_with_pos(Version, Bin, RecSize, 0, LastPos, Idx);
binary_search_index_hinted(Version, Bin, RecSize, _NumEntries, Idx,
                           {Idx, LastPos}) ->
    %% Same index as last time - direct lookup
    case parse_index_entry(Version, Bin, LastPos * RecSize) of
        {ok, {Idx, Term, DataOffset, Length, Crc}} ->
            {ok, {Term, DataOffset, Length, Crc}, LastPos};
        _ ->
            not_found
    end.

%% Binary search that returns position along with result
binary_search_index_with_pos(_Version, _Bin, _RecSize, Low, High, _Idx)
  when Low > High ->
    not_found;
binary_search_index_with_pos(Version, Bin, RecSize, Low, High, Idx) ->
    Mid = (Low + High) div 2,
    Offset = Mid * RecSize,
    case parse_index_entry(Version, Bin, Offset) of
        {ok, {MidIdx, Term, DataOffset, Length, Crc}} ->
            if MidIdx == Idx ->
                   {ok, {Term, DataOffset, Length, Crc}, Mid};
               MidIdx < Idx ->
                   binary_search_index_with_pos(Version, Bin, RecSize,
                                                Mid + 1, High, Idx);
               true ->
                   binary_search_index_with_pos(Version, Bin, RecSize,
                                                Low, Mid - 1, Idx)
            end;
        eof ->
            %% Hit unwritten region, search lower half
            binary_search_index_with_pos(Version, Bin, RecSize,
                                         Low, Mid - 1, Idx)
    end.

%% Map mode: use map lookups
read_sparse0(_Fd, [], _Index, _Cache, Acc, _AccFun, Num) ->
    {ok, Num, Acc};
read_sparse0(Fd, [NextIdx | Rem] = Indexes, Index, Cache0, Acc, AccFun, Num)
 when is_map_key(NextIdx, Index) ->
    {Term, Pos, Length, _} = map_get(NextIdx, Index),
    case cache_read(Cache0, Pos, Length) of
        false ->
            case prepare_cache(Fd, Indexes, Index) of
                undefined ->
                    %% TODO: check for partial data?
                    {ok, Data} = file:pread(Fd, Pos, Length),
                    read_sparse0(Fd, Rem, Index, undefined,
                                 AccFun(NextIdx, Term, Data, Acc),
                                 AccFun, Num+1);
                Cache ->
                    read_sparse0(Fd, Indexes, Index, Cache,
                                 Acc, AccFun, Num)
            end;
        Data ->
            read_sparse0(Fd, Rem, Index, Cache0,
                         AccFun(NextIdx, Term, Data, Acc), AccFun, Num+1)
    end;
read_sparse0(_Fd, [NextIdx | _], _Index, _Cache, _Acc, _AccFun, _Num) ->
    exit({missing_key, NextIdx}).

cache_read({CPos, CLen, Bin}, Pos, Length)
  when Pos >= CPos andalso
       Pos + Length =< (CPos + CLen) ->
    %% read fits inside cache
    binary:part(Bin, Pos - CPos, Length);
cache_read(_, _, _) ->
    false.

prepare_cache(_Fd, [_], _SegIndex) ->
    undefined;
prepare_cache(Fd, [FirstIdx | Rem], SegIndex) ->
    case consec_run(FirstIdx, FirstIdx, Rem) of
        {Idx, Idx} ->
            %% no run, no cache;
            undefined;
        {FirstIdx, LastIdx} ->
            {_, FstPos, FstLength, _} = map_get_(FirstIdx, SegIndex),
            {_, LastPos, LastLength, _} = map_get_(LastIdx, SegIndex),
            % %% read at least the remainder of the block from
            % %% the first position or the length of the first record
            CacheLen = cache_length(FstPos, FstLength, LastPos, LastLength),
            {ok, CacheData} = file:pread(Fd, FstPos, CacheLen),
            {FstPos, byte_size(CacheData), CacheData}
    end.

map_get_(Key, Map) when is_map_key(Key, Map) ->
    map_get(Key, Map);
map_get_(Key, _Map) ->
    exit({missing_key, Key}).

-spec term_query(state(), Idx :: ra_index()) -> option(ra_term()).
term_query(#state{index = Index,
                  num_entries = NumEntries,
                  cfg = #cfg{version = Version,
                             index_record_size = RecSize,
                             index_mode = IndexMode}}, Idx) ->
    case IndexMode of
        binary when is_binary(Index) ->
            %% Binary search on monotonically increasing index
            case binary_search_index(Version, Index, RecSize, 0,
                                     NumEntries - 1, Idx) of
                {ok, {Term, _, _, _}} ->
                    Term;
                not_found ->
                    undefined
            end;
        _ ->
            case Index of
                #{Idx := {Term, _, _, _}} ->
                    Term;
                _ ->
                    undefined
            end
    end.

fold0(_Cfg, _Cache, Idx, FinalIdx, _, _, _Fun, _AccFun, Acc, _)
  when Idx > FinalIdx ->
    Acc;
fold0(#cfg{index_mode = binary,
           version = Version,
           index_record_size = RecSize} = Cfg,
      Cache0, Idx, FinalIdx, Index, NumEntries, Fun, AccFun, Acc0,
      MissingKeyStrat)
  when is_binary(Index) ->
    %% Binary mode: use binary search
    case binary_search_index(Version, Index, RecSize, 0,
                             NumEntries - 1, Idx) of
        {ok, {Term, Offset, Length, Crc} = IdxRec} ->
            case pread(Cfg, Cache0, Offset, Length) of
                {ok, Data, Cache} ->
                    case validate_checksum(Crc, Data) of
                        true ->
                            Acc = AccFun({Idx, Term, Fun(Data)}, Acc0),
                            fold0(Cfg, Cache, Idx+1, FinalIdx,
                                  Index, NumEntries, Fun, AccFun, Acc,
                                  MissingKeyStrat);
                        false ->
                            exit({ra_log_segment_crc_check_failure, Idx, IdxRec,
                                  Cfg#cfg.filename})
                    end;
                {error, partial_data} ->
                    exit({ra_log_segment_unexpected_eof, Idx, IdxRec,
                          Cfg#cfg.filename})
            end;
        not_found when MissingKeyStrat == error ->
            exit({missing_key, Idx, Cfg#cfg.filename});
        not_found when MissingKeyStrat == return ->
            Acc0
    end;
fold0(Cfg, Cache0, Idx, FinalIdx, Index, NumEntries, Fun, AccFun,
      Acc0, MissingKeyStrat) ->
    %% Map mode: use map lookup
    case Index of
        #{Idx := {Term, Offset, Length, Crc} = IdxRec} ->
            case pread(Cfg, Cache0, Offset, Length) of
                {ok, Data, Cache} ->
                    %% perform crc check
                    case validate_checksum(Crc, Data) of
                        true ->
                            Acc = AccFun({Idx, Term, Fun(Data)}, Acc0),
                            fold0(Cfg, Cache, Idx+1, FinalIdx,
                                  Index, NumEntries, Fun, AccFun, Acc,
                                  MissingKeyStrat);
                        false ->
                            %% CRC check failures are irrecoverable
                            exit({ra_log_segment_crc_check_failure, Idx, IdxRec,
                                  Cfg#cfg.filename})
                    end;
                {error, partial_data} ->
                    %% we did not read the correct number of bytes suggesting
                    exit({ra_log_segment_unexpected_eof, Idx, IdxRec,
                          Cfg#cfg.filename})
            end;
        _ when MissingKeyStrat == error ->
            exit({missing_key, Idx, Cfg#cfg.filename});
        _ when MissingKeyStrat == return ->
            Acc0
    end.

-spec range(state()) -> option({ra_index(), ra_index()}).
range(#state{range = Range}) ->
    Range.

-spec max_count(state()) -> non_neg_integer().
max_count(#state{cfg = #cfg{max_count = Max}}) ->
    Max.

-spec filename(state()) -> file:filename().
filename(#state{cfg = #cfg{filename = Fn}}) ->
    Fn.

-spec segref(state() | file:filename_all()) ->
    option(ra_log:segment_ref()).
segref(#state{range = undefined}) ->
    undefined;
segref(#state{range = Range,
              cfg = #cfg{filename = Fn}}) ->
    {ra_lib:to_binary(filename:basename(Fn)), Range};
segref(Filename) ->
    {ok, Seg} = open(Filename, #{mode => read}),
    SegRef = segref(Seg),
    close(Seg),
    SegRef.

-type infos() :: #{size => non_neg_integer(),
                   index_size => non_neg_integer(),
                   max_count => non_neg_integer(),
                   file_type => regular | symlink,
                   ctime => integer(),
                   links => non_neg_integer(),
                   num_entries => non_neg_integer(),
                   ref => option(ra_log:segment_ref()),
                   indexes => ra_seq:state(),
                   live_size => non_neg_integer()
                  }.

-spec info(file:filename_all()) -> infos().
info(Filename) ->
    info(Filename, undefined).

-spec info(file:filename_all(), option(ra_seq:state())) -> infos().
info(Filename, Live0)
  when not is_tuple(Filename) ->
    %% Optimized implementation that parses index data in a single pass
    %% without building an intermediate map
    {ok, #file_info{type = Type,
                    links = Links,
                    ctime = CTime}} = prim_file:read_link_info(Filename,
                                                               [raw, {time, posix}]),
    {ok, Fd} = file:open(Filename, [read, raw, binary]),
    try
        {ok, Version, MaxCount} = read_header(Fd),
        IndexRecordSize = index_record_size(Version),
        IndexSize = MaxCount * IndexRecordSize,
        DataStart = ?HEADER_SIZE + IndexSize,
        %% Pass Live0 directly - ra_seq:in/2 is used for membership checks
        %% This avoids expanding the sequence to a set which could be expensive
        %% for large sequences. ra_seq:in/2 is efficient for compact sequences
        %% with ranges.
        case file:pread(Fd, ?HEADER_SIZE, IndexSize) of
            {ok, Data} ->
                {NumEntries, DataOffset, Range, IndexesSeq, LiveSize} =
                    parse_index_info(Version, Data, DataStart, Live0),
                Ref = case Range of
                          undefined -> undefined;
                          _ -> {ra_lib:to_binary(filename:basename(Filename)), Range}
                      end,
                #{size => DataOffset,
                  index_size => DataStart,
                  file_type => Type,
                  links => Links,
                  ctime => CTime,
                  max_count => MaxCount,
                  num_entries => NumEntries,
                  ref => Ref,
                  live_size => LiveSize,
                  indexes => IndexesSeq};
            eof ->
                %% Empty segment
                #{size => DataStart,
                  index_size => DataStart,
                  file_type => Type,
                  links => Links,
                  ctime => CTime,
                  max_count => MaxCount,
                  num_entries => 0,
                  ref => undefined,
                  live_size => 0,
                  indexes => []}
        end
    after
        _ = file:close(Fd)
    end.

-spec is_same_as(state(), file:filename_all()) -> boolean().
is_same_as(#state{cfg = #cfg{filename = Fn0}}, Fn) ->
    is_same_filename_all(Fn0, Fn).

-spec close(state()) -> ok.
close(#state{cfg = #cfg{fd = Fd,
                        mode = append,
                        file_advise = FileAdvise}} = State) ->
    % close needs to be defensive and idempotent so we ignore the return
    % values here
    _ = sync(State),
    _ = case is_full(State) of
            true ->
                file:advise(Fd, 0, 0, FileAdvise);
            false ->
                ok
        end,
    _ = file:close(Fd),
    ok;
close(#state{cfg = #cfg{fd = Fd}}) ->
    _ = file:close(Fd),
    ok.

-spec copy(state(), file:filename_all(), [ra:index()]) ->
    {ok, state()} | {error, term()}.
copy(#state{} = State0, FromFile, Indexes)
  when is_list(Indexes) ->
    {ok, From} = open(FromFile, #{mode => read}),
    %% TODO: the current approach recalculates the CRC and isn't completely
    %% optimial. Also it does not allow for a future where copy_file_range may
    %% be available
    State = lists:foldl(
              fun (I, S0) ->
                      {ok, Term, Data} = simple_read(From, I),
                      {ok, S} = append(S0, I, Term, Data),
                      S
              end, State0, lists:sort(Indexes)),
    close(From),
    sync(State).

%%% Internal

is_same_filename_all(Fn, Fn) ->
    true;
is_same_filename_all(Fn0, Fn1) ->
    B0 = filename:basename(Fn0),
    B1 = filename:basename(Fn1),
    ra_lib:to_binary(B0) == ra_lib:to_binary(B1).

update_range(undefined, Idx) ->
    {Idx, Idx};
update_range({First, _Last}, Idx) ->
    {min(First, Idx), Idx}.

recover_index(Fd, Version, MaxCount) ->
    IndexSize = MaxCount * index_record_size(Version),
    DataOffset = ?HEADER_SIZE + IndexSize,
    case file:pread(Fd, ?HEADER_SIZE, IndexSize) of
        {ok, Data} ->
            parse_index_data(Version, Data, DataOffset);
        eof ->
            % if no entries have been written the file hasn't "stretched"
            % to where the data offset starts.
            {0, DataOffset, undefined, #{}}
    end.

%% Binary mode: keep raw index binary, scan for range and count only
%% Returns {ok, NumEntries, DataOffset, Range, IndexBinary} if no overwrites,
%% or {fallback_to_map, NumEntries, DataOffset, Range, IndexMap} if overwrites detected.
recover_index_binary(Fd, Version, MaxCount) ->
    IndexSize = MaxCount * index_record_size(Version),
    DataOffset = ?HEADER_SIZE + IndexSize,
    case file:pread(Fd, ?HEADER_SIZE, IndexSize) of
        {ok, IndexBinary} ->
            case scan_index_binary(Version, IndexBinary, DataOffset) of
                {ok, NumEntries, ActualDataOffset, Range} ->
                    %% No overwrites - use binary mode
                    {ok, NumEntries, ActualDataOffset, Range, IndexBinary};
                {overwrites_detected, _PartialNum, _PartialOffset, _PartialRange} ->
                    %% Overwrites detected - fall back to map mode for correctness
                    %% Re-parse the full index into a map
                    {NumEntries, ActualDataOffset, Range, IndexMap} =
                        parse_index_data(Version, IndexBinary, DataOffset),
                    {fallback_to_map, NumEntries, ActualDataOffset, Range,
                     IndexMap}
            end;
        eof ->
            {ok, 0, DataOffset, undefined, <<>>}
    end.

%% Scan index binary to find range and count entries.
%% Returns {ok, NumEntries, DataOffset, Range} if no overwrites detected,
%% or {overwrites_detected, NumEntries, DataOffset, Range} if overwrites found.
%% When overwrites are detected, caller should fall back to map mode.
scan_index_binary(Version, Bin, DataOffset) ->
    scan_index_binary(Version, Bin, 0, 0, 0, DataOffset, undefined).

scan_index_binary(2, Bin, Offset, Num, LastIdx, DataOffset, Range)
  when byte_size(Bin) >= Offset + ?INDEX_RECORD_SIZE_V2 ->
    case Bin of
        <<_:Offset/binary, 0:64/unsigned, 0:64/unsigned, 0:64/unsigned,
          0:32/unsigned, 0:32/integer, _/binary>> ->
            {ok, Num, DataOffset, Range};
        <<_:Offset/binary, Idx:64/unsigned, _Term:64/unsigned,
          EntryOffset:64/unsigned, Length:32/unsigned,
          _Crc:32/integer, _/binary>> ->
            %% Detect overwrite: if Idx goes backwards, fall back to map mode
            case Idx < LastIdx of
                true ->
                    {overwrites_detected, Num + 1, EntryOffset + Length,
                     update_range(Range, Idx)};
                false ->
                    NewRange = update_range(Range, Idx),
                    NewDataOffset = EntryOffset + Length,
                    scan_index_binary(2, Bin, Offset + ?INDEX_RECORD_SIZE_V2,
                                      Num + 1, Idx, NewDataOffset, NewRange)
            end
    end;
scan_index_binary(1, Bin, Offset, Num, LastIdx, DataOffset, Range)
  when byte_size(Bin) >= Offset + ?INDEX_RECORD_SIZE_V1 ->
    case Bin of
        <<_:Offset/binary, 0:64/unsigned, 0:64/unsigned, 0:32/unsigned,
          0:32/unsigned, 0:32/integer, _/binary>> ->
            {ok, Num, DataOffset, Range};
        <<_:Offset/binary, Idx:64/unsigned, _Term:64/unsigned,
          EntryOffset:32/unsigned, Length:32/unsigned,
          _Crc:32/integer, _/binary>> ->
            case Idx < LastIdx of
                true ->
                    {overwrites_detected, Num + 1, EntryOffset + Length,
                     update_range(Range, Idx)};
                false ->
                    NewRange = update_range(Range, Idx),
                    NewDataOffset = EntryOffset + Length,
                    scan_index_binary(1, Bin, Offset + ?INDEX_RECORD_SIZE_V1,
                                      Num + 1, Idx, NewDataOffset, NewRange)
            end
    end;
scan_index_binary(_, _, _, Num, _, DataOffset, Range) ->
    {ok, Num, DataOffset, Range}.

%% Binary search on in-memory index binary
%% Returns {ok, {Term, Offset, Length, Crc}} or not_found
binary_search_index(_Version, _Bin, _RecSize, Low, High, _Idx)
  when Low > High ->
    not_found;
binary_search_index(Version, Bin, RecSize, Low, High, Idx) ->
    Mid = (Low + High) div 2,
    Offset = Mid * RecSize,
    case parse_index_entry(Version, Bin, Offset) of
        {ok, {MidIdx, Term, DataOffset, Length, Crc}} ->
            if MidIdx == Idx ->
                   {ok, {Term, DataOffset, Length, Crc}};
               MidIdx < Idx ->
                   binary_search_index(Version, Bin, RecSize, Mid + 1,
                                       High, Idx);
               true ->
                   binary_search_index(Version, Bin, RecSize, Low, Mid - 1, Idx)
            end;
        eof ->
            %% Hit unwritten region, search lower half
            binary_search_index(Version, Bin, RecSize, Low, Mid - 1, Idx)
    end.

%% Parse an index entry from a binary at the given offset
parse_index_entry(2, Bin, Offset)
  when byte_size(Bin) >= Offset + ?INDEX_RECORD_SIZE_V2 ->
    case Bin of
        <<_:Offset/binary, 0:64/unsigned, 0:64/unsigned, 0:64/unsigned,
          0:32/unsigned, 0:32/integer, _/binary>> ->
            eof;
        <<_:Offset/binary, Idx:64/unsigned, Term:64/unsigned,
          DataOffset:64/unsigned, Length:32/unsigned,
          Crc:32/integer, _/binary>> ->
            {ok, {Idx, Term, DataOffset, Length, Crc}}
    end;
parse_index_entry(1, Bin, Offset)
  when byte_size(Bin) >= Offset + ?INDEX_RECORD_SIZE_V1 ->
    case Bin of
        <<_:Offset/binary, 0:64/unsigned, 0:64/unsigned, 0:32/unsigned,
          0:32/unsigned, 0:32/integer, _/binary>> ->
            eof;
        <<_:Offset/binary, Idx:64/unsigned, Term:64/unsigned,
          DataOffset:32/unsigned, Length:32/unsigned,
          Crc:32/integer, _/binary>> ->
            {ok, {Idx, Term, DataOffset, Length, Crc}}
    end;
parse_index_entry(_, _, _) ->
    eof.

dump_index(File) ->
    {ok, Fd} = file:open(File, [read, raw, binary
                               ]),
    {ok, Version, MaxCount} = read_header(Fd),
    IndexSize = MaxCount * index_record_size(Version),
    case file:pread(Fd, ?HEADER_SIZE, IndexSize) of
        {ok, Data} ->
            D = [{I, T, O} || {I, T, O, _N} <- dump_index_data(Data, [])],
            _ = file:close(Fd),
            D;
        eof ->
            _ = file:close(Fd),
            % if no entries have been written the file hasn't "stretched"
            % to where the data offset starts.
            DataOffset = ?HEADER_SIZE + IndexSize,
            {0, DataOffset, undefined, #{}}
    end.

dump(File) ->
    dump(File, fun (B) -> B end).

dump(File, Fun) ->
    {ok, S0} = open(File, #{mode => read}),
    {Idx, Last} = range(S0),
    L = fold(S0, Idx, Last, Fun,
             fun (E, A) -> [E | A] end, []),
    close(S0),
    lists:reverse(L).


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

%% Optimized index parsing for info/2 that computes stats in a single pass
%% without building a full index map. Returns:
%% {NumEntries, DataOffset, Range, IndexesSeq, LiveSize}
parse_index_info(2, Data, DataOffset, LiveSeq) ->
    parse_index_info_v2(Data, 0, 0, DataOffset, undefined, [], 0, LiveSeq);
parse_index_info(1, Data, DataOffset, LiveSeq) ->
    parse_index_info_v1(Data, 0, 0, DataOffset, undefined, [], 0, LiveSeq).

%% V2 format: 64-bit index, 64-bit term, 64-bit offset, 32-bit length,
%% 32-bit crc
parse_index_info_v2(<<>>, Num, _LastIdx, DataOffset, Range, IdxAcc, LiveSize,
                    _LiveSeq) ->
    {Num, DataOffset, Range, ra_seq:from_list(lists:reverse(IdxAcc)), LiveSize};
parse_index_info_v2(<<0:64/unsigned, 0:64/unsigned, 0:64/unsigned,
                      0:32/unsigned, 0:32/integer, _Rest/binary>>,
                    Num, _LastIdx, DataOffset, Range, IdxAcc, LiveSize,
                    _LiveSeq) ->
    %% End of written data (partially written index)
    {Num, DataOffset, Range, ra_seq:from_list(lists:reverse(IdxAcc)), LiveSize};
parse_index_info_v2(<<Idx:64/unsigned, _Term:64/unsigned,
                      Offset:64/unsigned, Length:32/unsigned,
                      _Crc:32/integer, Rest/binary>>,
                    Num, LastIdx, _DataOffset, Range, IdxAcc0, LiveSize0,
                    LiveSeq) ->
    %% Handle index going backwards (trim entries)
    IdxAcc = case Idx < LastIdx of
                 true ->
                     lists:dropwhile(fun(I) -> I > Idx end, IdxAcc0);
                 false ->
                     IdxAcc0
             end,
    %% Compute live size: if LiveSeq is undefined, all entries are live
    LiveSize = case LiveSeq of
                   undefined ->
                       LiveSize0 + Length;
                   _ ->
                       case ra_seq:in(Idx, LiveSeq) of
                           true ->
                               LiveSize0 + Length;
                           false ->
                               LiveSize0
                       end
               end,
    parse_index_info_v2(Rest, Num+1, Idx,
                        Offset + Length,
                        update_range(Range, Idx),
                        [Idx | IdxAcc], LiveSize, LiveSeq).

%% V1 format: 64-bit index, 64-bit term, 32-bit offset, 32-bit length, 32-bit crc
parse_index_info_v1(<<>>, Num, _LastIdx, DataOffset, Range, IdxAcc, LiveSize, _LiveSeq) ->
    {Num, DataOffset, Range, ra_seq:from_list(lists:reverse(IdxAcc)), LiveSize};
parse_index_info_v1(<<0:64/unsigned, 0:64/unsigned, 0:32/unsigned,
                      0:32/unsigned, 0:32/integer, _Rest/binary>>,
                    Num, _LastIdx, DataOffset, Range, IdxAcc, LiveSize, _LiveSeq) ->
    %% End of written data (partially written index)
    {Num, DataOffset, Range, ra_seq:from_list(lists:reverse(IdxAcc)), LiveSize};
parse_index_info_v1(<<Idx:64/unsigned, _Term:64/unsigned,
                      Offset:32/unsigned, Length:32/unsigned,
                      _Crc:32/integer, Rest/binary>>,
                    Num, LastIdx, _DataOffset, Range, IdxAcc0, LiveSize0, LiveSeq) ->
    %% Handle index going backwards (trim entries)
    IdxAcc = case Idx < LastIdx of
                 true -> lists:dropwhile(fun(I) -> I > Idx end, IdxAcc0);
                 false -> IdxAcc0
             end,
    %% Compute live size: if LiveSeq is undefined, all entries are live
    LiveSize = case LiveSeq of
                   undefined -> LiveSize0 + Length;
                   _ -> case ra_seq:in(Idx, LiveSeq) of
                            true -> LiveSize0 + Length;
                            false -> LiveSize0
                        end
               end,
    parse_index_info_v1(Rest, Num+1, Idx,
                        Offset + Length,
                        update_range(Range, Idx),
                        [Idx | IdxAcc], LiveSize, LiveSeq).

write_header(MaxCount, Fd) ->
    Header = <<?MAGIC, ?VERSION:16/unsigned, MaxCount:16/unsigned>>,
    {ok, 0} = file:position(Fd, 0),
    ok = file:write(Fd, Header),
    ok = ra_file:sync(Fd).

read_header(Fd) ->
    case file:pread(Fd, 0, ?HEADER_SIZE) of
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

pread(#cfg{access_pattern = random,
           fd = Fd}, Cache, Pos, Length) ->
    %% no cache
    {ok, Data} = file:pread(Fd, Pos, Length),
    case byte_size(Data)  of
        Length ->
            {ok, Data, Cache};
        _ ->
            {error, partial_data}
    end;
pread(#cfg{}, {CPos, CLen, Bin} = Cache, Pos, Length)
  when Pos >= CPos andalso
       Pos + Length =< (CPos + CLen) ->
    %% read fits inside cache
    {ok, binary:part(Bin, Pos - CPos, Length), Cache};
pread(#cfg{access_pattern = sequential,
           fd = Fd} = Cfg, undefined, Pos, Length) ->
    CacheLen = max(Length, ?READ_AHEAD_B),
    {ok, CacheData} = file:pread(Fd, Pos, CacheLen),
    case byte_size(CacheData) >= Length  of
        true ->
            pread(Cfg, {Pos, byte_size(CacheData), CacheData}, Pos, Length);
        false ->
            {error, partial_data}
    end;
pread(Cfg, {_, _, _}, Pos, Length) ->
    %% invalidate cache
    pread(Cfg, undefined, Pos, Length).


offset_size(2) -> 64;
offset_size(1) -> 32.

index_record_size(2) ->
    ?INDEX_RECORD_SIZE_V2;
index_record_size(1) ->
    ?INDEX_RECORD_SIZE_V1.

%% returns the first and last indexes of the next consecutive run
%% of indexes
consec_run(First, Last, []) ->
    {First, Last};
consec_run(First, Last, [Next | Rem])
  when Next == Last + 1 ->
    consec_run(First, Next, Rem);
consec_run(First, Last, _) ->
    {First, Last}.

cache_length(FstPos, FstLength, LastPos, LastLength) ->
    %% The cache needs to be at least as large as the next entry
    %% but no larger than ?READ_AHEAD_B
    MaxCacheLen = LastPos + LastLength - FstPos,
    %% read at least the remainder of the block from
    %% the first position or the length of the first record
    MinCacheLen = max(FstLength, ?BLOCK_SIZE - (FstPos rem ?BLOCK_SIZE)),
    max(MinCacheLen, min(MaxCacheLen, ?READ_AHEAD_B)).

compute_checksum(#cfg{compute_checksums = false}, _) ->
    0;
compute_checksum(#cfg{}, Data) ->
    erlang:crc32(Data).

validate_checksum(0, _) ->
    true;
validate_checksum(Crc, Data) ->
    Crc == erlang:crc32(Data).

is_full(#state{cfg = #cfg{max_size = MaxSize},
               index_offset = IndexOffset,
               data_start = DataStart,
               data_offset = DataOffset}) ->
    IndexOffset >= DataStart orelse
    (DataOffset - DataStart) > MaxSize.

simple_read(#state{cfg = #cfg{fd = Fd},
                   index = SegIndex}, Idx)
  when is_map_key(Idx, SegIndex) ->
    {Term, Pos, Len, _} = map_get(Idx, SegIndex),
    case file:pread(Fd, Pos, Len) of
        {ok, Data} ->
            ?assert(byte_size(Data) == Len),
            {ok, Term, Data};
        Err ->
            Err
    end;
simple_read(_State, _) ->
    {error, not_found}.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

cache_length_test() ->
    B = ?BLOCK_SIZE,
    B2 = ?BLOCK_SIZE * 2,
    %% total request is smaller than block size
    ?assertEqual(B, cache_length(B, 10, B + 10, 10)),
    %% larger than block size
    ?assertEqual(B2, cache_length(B, B, B + B, B)),

    %% large first entry
    ?assertEqual(?READ_AHEAD_B * 2, cache_length(B, ?READ_AHEAD_B * 2,
                                                 ?READ_AHEAD_B * 4, B)),

    %% if the request is oversized, return the max read ahead as cache size
    ?assertEqual(?READ_AHEAD_B, cache_length(B, B, ?READ_AHEAD_B * 2, B)),
    ok.

-endif.


