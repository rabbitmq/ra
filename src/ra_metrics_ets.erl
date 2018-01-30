-module(ra_metrics_ets).
-behaviour(gen_server).

-export([start_link/0,
         make_table/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {}).

-include("ra.hrl").

%%% here to own metrics ETS tables

%%%===================================================================
%%% API functions
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

make_table(Table) when is_atom(Table) ->
    case ets:info(Table) of
        undefined ->
            gen_server:call(?MODULE, {make_table, Table});
        _ ->
            ok
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    _ = ets:new(ra_log_file_metrics, [named_table, set, public,
                                      {write_concurrency, true},
                                      {read_concurrency, true}]),
    {ok, #state{}}.

handle_call({make_table, Table}, _From, State) ->
    ?INFO("ra_metrics_ets creating new table ~p~n", [Table]),
    _ = ets:new(Table, [named_table, set, public,
                        {write_concurrency, true},
                        {read_concurrency, true}]),
    {reply, ok, State}.

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
