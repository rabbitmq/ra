-module(ra_log_file_snapshot_writer).

-behaviour(gen_server).

-include("ra.hrl").

%% API functions
-export([start_link/0,
         write_snapshot/3]).

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
write_snapshot(From, Dir, Snapshot) ->
    gen_server:cast(?MODULE, {write_snapshot, From, Dir, Snapshot}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({write_snapshot, From, Dir, Snapshot}, State) ->
    ok = handle_write_snapshot(From, Dir, Snapshot),
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

handle_write_snapshot(From, Dir, {Index, Term, _, _} = Snapshot) ->
    case lists:sort(filelib:wildcard(filename:join(Dir, "*.snapshot"))) of
        [] ->
            % no snapshots - initialise snapshot counter
            File = filename:join(Dir, ra_lib:zpad_filename("", "snapshot", 1)),
            ok = write(File, Snapshot),
            From ! {ra_log_event, {snapshot_written, {Index, Term}, File}},
            ok;
        [LastFile | _] = Old ->
            File = ra_lib:zpad_filename_incr(LastFile),
            ok = write(File, Snapshot),
            From ! {ra_log_event, {snapshot_written, {Index, Term}, File}},
            % delete old snapshots
            [file:delete(F) || F <- Old],
            ok
    end.

write(File, Snapshot) ->
    ?DBG("Writing snapshot file ~p", [File]),
    file:write_file(File, term_to_binary(Snapshot)).
