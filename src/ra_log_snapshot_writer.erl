%% @hidden
-module(ra_log_snapshot_writer).

-behaviour(gen_server).

-include("ra.hrl").


%% API functions
-export([start_link/0,
         write_snapshot/5,
         save_snapshot_call/4
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
-spec write_snapshot(pid(), file:filename_all(), ra_snapshot:meta(), term(), module()) -> ok.
write_snapshot(From, Dir, Meta, Ref, Module) ->
    gen_server:cast(?MODULE, {write_snapshot, From, Dir, Meta, Ref, Module}).

-spec save_snapshot_call(file:filename(), ra_snapshot:meta(), term(), module()) ->
    {ok, File :: file:filename(), Old :: [file:filename()]}.
save_snapshot_call(Dir, Meta, Data, Module) ->
    gen_server:call(?MODULE, {save_snapshot, Dir, Meta, Data, Module}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{}}.

handle_call({save_snapshot, Dir, Meta, Data, Module}, _From, State) ->
    Reply = with_filename_incr(Dir, fun(File) ->
        save(File, Meta, Data, Module)
    end),
    {reply, Reply, State}.

handle_cast({write_snapshot, From, Dir, {Index, Term, _} = Meta, Ref, Module}, State) ->
    {ok, File, Old} = with_filename_incr(Dir, fun(File) ->
        write(File, Meta, Ref, Module)
    end),
    From ! {ra_log_event, {snapshot_written, {Index, Term}, File, Old}},
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

write(File, Meta, Ref, Module) ->
    % TODO: snapshots should be checksummed
    ?INFO("snapshot_writer: Writing file ~p", [File]),
    Module:write(File, Meta, Ref).

save(File, Meta, Data, Module) ->
    % TODO: snapshots should be checksummed
    ?INFO("snapshot_writer: Writing file ~p", [File]),
    Module:save(File, Meta, Data).
