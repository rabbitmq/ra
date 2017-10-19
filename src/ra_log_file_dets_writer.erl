-module(ra_log_file_dets_writer).
-behaviour(gen_server).

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

%%% ra_log_file_dets_writer
%%% receives a set of mem_tables from the wal
%%% appends to the current dets table for the target and flushes
%%% notifies ra_log_file_reader if a new dets tables were created
%%% removes the table from ra_log_closed_mem_tables
%%% schedules ETS table deletions
%%% processes compaction requests
%%% TODO: work out how overwrites should work and synchronise with readers

%%%===================================================================
%%% API functions
%%%===================================================================

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

handle_cast(_Msg, State) ->
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
