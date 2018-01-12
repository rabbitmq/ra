-module(ra_log_file_snapshot_writer).

-behaviour(gen_server).

-include("ra.hrl").

%% API functions
-export([start_link/0,
         write_snapshot/3,
         write_snapshot_call/2
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
write_snapshot(From, Dir, Snapshot) ->
    gen_server:cast(?MODULE, {write_snapshot, From, Dir, Snapshot}).

write_snapshot_call(Dir, Snapshot) ->
    gen_server:call(?MODULE, {write_snapshot, Dir, Snapshot}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({write_snapshot, Dir, Snapshot}, _From, State) ->
    Reply = write_snapshot(Dir, Snapshot),
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
    {ok, File} = write_snapshot(Dir, Snapshot),
    From ! {ra_log_event, {snapshot_written, {Index, Term}, File}},
    ok.

write_snapshot(Dir, Snapshot) ->
    case lists:sort(filelib:wildcard(filename:join(Dir, "*.snapshot"))) of
        [] ->
            % no snapshots - initialise snapshot counter
            File = filename:join(Dir, ra_lib:zpad_filename("", "snapshot", 1)),
            ok = write(File, Snapshot),
            {ok, File};
        [LastFile | _] = Old ->
            File = ra_lib:zpad_filename_incr(LastFile),
            ok = write(File, Snapshot),
            % delete old snapshots
            [file:delete(F) || F <- Old],
            {ok, File}
    end.

write(File, Snapshot) ->
    % TODO: snapshots should be checksummed
    ?INFO("snapshot_writer: Writing file ~p", [File]),
    file:write_file(File, term_to_binary(Snapshot)).
