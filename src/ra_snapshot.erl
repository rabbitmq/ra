-module(ra_snapshot).

-include("ra.hrl").

-type file_err() :: file:posix() | badarg | terminated | system_limit.
-type meta() :: {ra_index(), ra_term(), ra_cluster_servers()}.

-export([
         save/4,
         read/2,
         recover/1,
         read_meta/2,
         delete/2,

         init/3,
         init_ets/0,
         current/1,
         pending/1,
         accepting/1,
         directory/1,
         last_index_for/1,

         begin_snapshot/3,
         complete_snapshot/2,

         begin_accept/4,
         accept_chunk/3,
         abort_accept/1,

         handle_down/3
        ]).

-type effect() :: {monitor, process, snapshot_writer, pid()}.

-export_type([meta/0, file_err/0, effect/0]).

-record(accept, {num :: non_neg_integer(),
                 %% the next expected chunk
                 next = 1 :: non_neg_integer(),
                 state :: term(),
                 idxterm :: ra_idxterm()}).

-record(?MODULE,
        {uid :: ra_uid(),
         module :: module(),
         %% the snapshot directory
         %% typically <data_dir>/snapshots
         %% snapshot subdirs are store below
         %% this as <data_dir>/snapshots/Term_Index
         directory = file:filename(),
         pending :: maybe({pid(), ra_idxterm()}),
         accepting :: maybe(#accept{}),
         current :: maybe(ra_idxterm())}).

%% getter macro
-define(GETTER(State), State#?MODULE.?FUNCTION_NAME).

-define(ETSTBL, ra_log_snapshot_state).

-opaque state() :: #?MODULE{}.

-export_type([state/0]).

%% Side effect function
%% Turn the current state into immutable reference.
-callback prepare(Index :: ra_index(),
                  State :: term()) ->
    Ref :: term().

%% Saves snapshot from external state to disk.
%% Runs in a separate process.
%% External storage should be available to read
-callback write(Location :: file:filename(),
                Meta :: meta(),
                Ref :: term()) ->
    ok | {error, file_err() | term()}.

%% Read the snapshot from disk into serialised structure for transfer.
-callback read(ChunkSizeBytes :: non_neg_integer(),
               Location :: file:filename()) ->
    {ok, Meta :: meta(), ChunkState,
     [ChunkThunk :: fun((ChunkState) -> {binary(), ChunkState})]} |
    {error, term()}.

%% begin a stateful snapshot acceptance process
-callback begin_accept(SnapDir :: file:filename(),
                       Crc :: non_neg_integer(),
                       Meta :: meta()) ->
    {ok, AcceptState :: term()} | {error, term()}.

%% accept a chunk of data
-callback accept_chunk(Data :: binary(),
                       AcceptState :: term()) ->
    {ok, AcceptState :: term()} | {error, term()}.

%% accept the last chunk of data
-callback complete_accept(Data :: binary(),
                          AcceptState :: term()) ->
    ok | {error, term()}.

%% Side-effect function
%% Recover machine state from file
-callback recover(Location :: file:filename()) ->
    {ok, Meta :: meta(), State :: term()} | {error, term()}.

%% validate the integrity of the snapshot
-callback validate(Location :: file:filename()) ->
    ok | {error, term()}.

%% Only read meta data from snapshot
-callback read_meta(Location :: file:filename()) ->
    {ok, meta()} |
    {error, invalid_format |
            {invalid_version, integer()} |
            checksum_error |
            file_err() |
            term()}.

-spec init(ra_uid(), module(), file:filename()) ->
    state().
init(UId, Module, SnapshotsDir) ->
    State = #?MODULE{uid = UId,
                     module = Module,
                     directory = SnapshotsDir},
    true = filelib:is_dir(SnapshotsDir),
    {ok, Snaps0} = file:list_dir(SnapshotsDir),
    Snaps = lists:reverse(lists:sort(Snaps0)),
    case pick_first_valid(UId, Module, SnapshotsDir, Snaps) of
        undefined ->
            ok = delete_snapshots(SnapshotsDir, Snaps),
            State;
        Current0 ->
            ?INFO("ra_snapshot: recovering ~s", [Current0]),
            Current = filename:join(SnapshotsDir, Current0),
            %% TODO: validate Current snapshot integrity before accepting it as
            %% current
            {ok, {Idx, Term, _}} = Module:read_meta(Current),
            true = ets:insert(?ETSTBL, {UId, Idx}),

            ok = delete_snapshots(SnapshotsDir, lists:delete(Current0, Snaps)),
            %% delete old snapshots if any
            State#?MODULE{current = {Idx, Term}}
    end.

delete_snapshots(Dir, Snaps) ->
    % ?INFO("ra_snapshot: deleting ~s in ~s", [Snaps, Dir]),
    Old = [filename:join(Dir, O) || O <- Snaps],
    lists:foreach(fun ra_lib:recursive_delete/1, Old),
    ok.

pick_first_valid(_, _, _, []) ->
    undefined;
pick_first_valid(UId, Mod, Dir, [S | Rem]) ->
    case Mod:validate(filename:join(Dir, S)) of
        ok -> S;
        Err ->
            ?INFO("ra_snapshot: ~s: skipping ~s as did not validate. Err: ~w~n",
                  [UId, S, Err]),
            pick_first_valid(UId, Mod, Dir, Rem)
    end.


-spec init_ets() -> ok.
init_ets() ->
    TableFlags = [set,
                  named_table,
                  {read_concurrency, true},
                  {write_concurrency, true},
                  public],
    _ = ets:new(?ETSTBL, TableFlags),
    ok.

-spec current(state()) -> maybe(ra_idxterm()).
current(State) -> ?GETTER(State).

-spec pending(state()) -> maybe({pid(), ra_idxterm()}).
pending(State) -> ?GETTER(State).

-spec accepting(state()) -> maybe(ra_idxterm()).
accepting(State) ->
    case ?GETTER(State) of
        undefined ->
            undefined;
        #accept{idxterm = IdxTerm} ->
            IdxTerm
    end.

-spec directory(state()) -> file:filename().
directory(State) -> ?GETTER(State).

-spec last_index_for(ra_uid()) -> maybe(ra_index()).
last_index_for(UId) ->
    case ets:lookup(?ETSTBL, UId) of
        [] -> undefined;
        [{_, Index}] -> Index
    end.

-spec begin_snapshot(meta(), ReleaseCursorRef :: term(), state()) ->
    {state(), [effect()]}.
begin_snapshot({Idx, Term, _Cluster} = Meta, MacRef,
               #?MODULE{module = Mod,
                        directory = Dir} = State) ->
    %% create directory for this snapshot
    SnapDir = make_snapshot_dir(Dir, Idx, Term),
    ok = file:make_dir(SnapDir),
    %% call prepare then write_snapshot
    %% This needs to be called in the current process to "lock" potentially
    %% mutable machine state
    Ref = Mod:prepare(Meta, MacRef),
    %% write the snapshot in a separate process
    Self = self(),
    Pid = spawn (fun () ->
                         ok = Mod:write(SnapDir, Meta, Ref),
                         Self ! {ra_log_event,
                                 {snapshot_written, {Idx, Term}}},
                         ok
                 end),

    %% record snapshot in progress
    %% emit an effect that monitors the current snapshot attempt
    {State#?MODULE{pending = {Pid, {Idx, Term}}},
     [{monitor, process, snapshot_writer, Pid}]}.

-spec complete_snapshot(ra_idxterm(), state()) ->
    state().
complete_snapshot({Idx, _} = IdxTerm,
                  #?MODULE{uid = UId,
                           module = _Mod,
                           directory = _Dir} = State) ->
    true = ets:insert(?ETSTBL, {UId, Idx}),
    State#?MODULE{pending = undefined,
                  current = IdxTerm}.

-spec begin_accept(Crc :: non_neg_integer(), meta(),
                   NumChunks :: non_neg_integer(), state()) ->
    {ok, state()}.
begin_accept(Crc, {Idx, Term, _} = Meta, NumChunks,
             #?MODULE{module = Mod,
                      directory = Dir} = State) ->
    SnapDir = make_snapshot_dir(Dir, Idx, Term),
    ok = file:make_dir(SnapDir),
    {ok, AcceptState} = Mod:begin_accept(SnapDir, Crc, Meta),
    {ok, State#?MODULE{accepting = #accept{idxterm = {Idx, Term},
                                           num = NumChunks,
                                           state = AcceptState}}}.

-spec accept_chunk(binary(), non_neg_integer(), state()) ->
    {ok, state()}.
accept_chunk(Data, Num,
             #?MODULE{uid = UId,
                      module = Mod,
                      directory = Dir,
                      current = Current,
                      accepting = #accept{num = Num,
                                          idxterm = {Idx, _} = IdxTerm,
                                          state = AccState}} = State) ->
    %% last chunk
    ok = Mod:complete_accept(Data, AccState),
    %% run validate here?
    %% delete the current snapshot if any
    ok = delete(Dir, Current),
    %% update ets table
    true = ets:insert(?ETSTBL, {UId, Idx}),
    {ok, State#?MODULE{accepting = undefined,
                       current = IdxTerm}};
accept_chunk(Data, Num,
             #?MODULE{module = Mod,
                      accepting =
                      #accept{state = AccState0,
                              next = Num} = Accept} = State) ->
    {ok, AccState} = Mod:accept_chunk(Data, AccState0),
    {ok, State#?MODULE{accepting = Accept#accept{state = AccState,
                                                 next = Num + 1}}};
accept_chunk(_Data, Num,
             #?MODULE{accepting = #accept{next = Next}} = State)
  when Next > Num ->
    %% this must be a resend - we can just ignore it
    {ok, State}.

-spec abort_accept(state()) -> state().
abort_accept(#?MODULE{accepting = undefined} = State) ->
    State;
abort_accept(#?MODULE{accepting = #accept{idxterm = {Idx, Term}},
                      directory = Dir} = State) ->
    SnapDir = make_snapshot_dir(Dir, Idx, Term),
    _ = ra_lib:recursive_delete(SnapDir),
    State#?MODULE{accepting = undefined}.


-spec handle_down(pid(), Info :: term(), state()) ->
    state().
handle_down(_Pid, _Info, #?MODULE{pending = undefined} = State) ->
    State;
handle_down(_Pid, normal, State) ->
    State;
handle_down(_Pid, noproc, State) ->
    %% this could happen if the monitor was set up after the process had
    %% finished
    State;
handle_down(Pid, _Info,
            #?MODULE{directory = Dir,
                     pending = {Pid, IdxTerm}} = State) ->
    %% delete the pending snapshot directory
    ok = delete(Dir, IdxTerm),
    State#?MODULE{pending = undefined}.

delete(_, undefined) ->
    ok;
delete(Dir, {Idx, Term}) ->
    SnapDir = make_snapshot_dir(Dir, Idx, Term),
    ok = ra_lib:recursive_delete(SnapDir),
    ok.

-spec save(Module :: module(), Location :: file:filename(),
           Meta :: meta(), Data :: term()) ->
    ok | {error, file_err() | term()}.
save(Module, Location, Meta, Data) ->
    Module:save(Location, Meta, Data).


-spec read(ChunkSizeBytes :: non_neg_integer(), State :: state()) ->
    {ok, Crc :: non_neg_integer(), Meta :: meta(), ChunkState,
     [ChunkThunk :: fun((ChunkState) -> {binary(), ChunkState})]} |
    {error, term()} when ChunkState :: term().
read(ChunkSizeBytes, #?MODULE{module = Mod,
                              directory = Dir,
                              current = {Idx, Term}}) ->
    Location = make_snapshot_dir(Dir, Idx, Term),
    Mod:read(ChunkSizeBytes, Location).

-spec recover(state()) ->
    {ok, Meta :: meta(), State :: term()} |
    {error, no_current_snapshot} |
    {error, term()}.
recover(#?MODULE{current = undefined}) ->
    {error, no_current_snapshot};
recover(#?MODULE{module = Mod,
                 directory = Dir,
                 current = {Idx, Term}}) ->
    SnapDir = make_snapshot_dir(Dir, Idx, Term),
    Mod:recover(SnapDir).

-spec read_meta(Module :: module(), Location :: file:filename()) ->
    {ok, meta()} |
    {error, invalid_format |
            {invalid_version, integer()} |
            checksum_error |
            file_err() |
            term()}.
read_meta(Module, Location) ->
    Module:read_meta(Location).

make_snapshot_dir(Dir, Index, Term) ->
    I = ra_lib:zpad_hex(Index),
    T = ra_lib:zpad_hex(Term),
    filename:join(Dir, T ++ "_" ++ I).
