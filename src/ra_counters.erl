-module(ra_counters).

-export([
         init/0,
         new/2,
         register/3,
         overview/0
         ]).

%% holds static or rarely changing fields
-record(cfg, {}).

-record(?MODULE, {cfg :: #cfg{}}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

init() ->
    _ = ets:new(?MODULE, [set, named_table, public]),
    ok.

new(Name, Size) ->
    CRef = counters:new(Size, []),
    register(Name, CRef, Size),
    CRef.


register(Name, Ref, Size) ->
    ets:insert(?MODULE, {Name, Ref, Size}).

overview() ->
    ets:foldl(
      fun({Name, Ref, Size}, Acc) ->
              Values = [counters:get(Ref, I) || I <- lists:seq(1, Size)],
              Acc#{Name => Values}
      end, #{}, ?MODULE).




-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
