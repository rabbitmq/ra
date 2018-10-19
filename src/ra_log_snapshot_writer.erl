%% @hidden
-module(ra_log_snapshot_writer).

-behaviour(gen_server).

-include("ra.hrl").


%% API functions
-export([start_link/0,
         write_snapshot/4,
         save_snapshot_call/3
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

%%%===================================================================
%%% API functions
%%%===================================================================
-spec write_snapshot(pid(), file:filename_all(), ra_snapshot:meta(), term()) -> ok.
write_snapshot(From, Dir, Meta, Ref) ->
    gen_server:cast(?MODULE, {write_snapshot, From, Dir, Meta, Ref}).

-spec save_snapshot_call(file:filename(), ra_snapshot:meta(), term()) ->
    {ok, File :: file:filename(), Old :: [file:filename()]}.
save_snapshot_call(Dir, Meta, Data) ->
    gen_server:call(?MODULE, {save_snapshot, Dir, Meta, Data}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({save_snapshot, Dir, Meta, Data}, _From, State) ->
    Reply = save_snapshot(Dir, Meta, Data),
    {reply, Reply, State}.

handle_cast({write_snapshot, From, Dir, Meta, Ref}, State) ->
    ok = handle_write_snapshot(From, Dir, Meta, Ref),
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

handle_write_snapshot(From, Dir, {Index, Term, _} = Meta, Ref) ->
    {ok, File, Old} = write_snapshot(Dir, Meta, Ref),
    From ! {ra_log_event, {snapshot_written, {Index, Term}, File, Old}},
    ok.

write_snapshot(Dir, Meta, Ref) ->
    with_filename_incr(Dir, fun(File) ->
        write(File, Meta, Ref)
    end).

save_snapshot(Dir, Meta, Data) ->
    with_filename_incr(Dir, fun(File) ->
        save(File, Meta, Data)
    end).

with_filename_incr(Dir, WriteFun) ->
    case lists:sort(filelib:wildcard(filename:join(Dir, "*.snapshot"))) of
        [] ->
            % no snapshots - initialise snapshot counter
            File = filename:join(Dir, ra_lib:zpad_filename("", "snapshot", 1)),
            ok = WriteFun(File),
            {ok, File, []};
        [LastFile | _] = Old ->
            File = ra_lib:zpad_filename_incr(LastFile),
            ok = WriteFun(File),
            {ok, File, Old}
    end.

write(File, Meta, Ref) ->
    % TODO: snapshots should be checksummed
    ?INFO("snapshot_writer: Writing file ~p", [File]),
    ra_log_snapshot:write(File, Meta, Ref).

save(File, Meta, Data) ->
    % TODO: snapshots should be checksummed
    ?INFO("snapshot_writer: Writing file ~p", [File]),
    ra_log_snapshot:save(File, Meta, Data).
