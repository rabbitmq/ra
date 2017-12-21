-module(ra_log_file_ets).
-behaviour(gen_server).

-export([start_link/0,
         give_away/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("ra.hrl").

-record(state, {}).

%%% ra_log_file_ets - owns mem_table ETS tables

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec give_away(ets:tid()) -> true.
give_away(Tid) ->
    ets:give_away(Tid, whereis(?MODULE), undefined).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    % create mem table lookup table to be used to map ra cluster name
    % to table identifiers to query.
    _ = ets:new(ra_log_open_mem_tables,
                [set, named_table, {read_concurrency, true}, public]),
    _ = ets:new(ra_log_closed_mem_tables,
                [bag, named_table, {read_concurrency, true}, public]),
    {ok, #state{}}.

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Info, State) ->
    ?INFO("~p: info msg: ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================






