-module(ra_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
     basic
    ].

basic(_Config) ->
    Self = self(),
    [{APid, _A}, _B, _C] = ra:start_cluster(
                             3, "test", fun(Cmd, State) ->
                                                ct:pal("Applying ~p to ~p", [Cmd, State]),
                                                Self ! applied,
                                                State + Cmd end, 0),

    {1, 1} = ra:command(APid, 5),
    receive
        applied -> ok
    after 3000 ->
              exit(apply_timeout)
    end,
    ok.
