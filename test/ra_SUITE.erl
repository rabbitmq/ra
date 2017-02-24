-module(ra_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
     basic
    ].

basic(_Config) ->
    Self = self(),
    [{APid, _A}, _B, _C] =
    ra:start_cluster(3, "test",
                     fun(Cmd, State) ->
                             ct:pal("Applying ~p to ~p", [Cmd, State]),
                             Self ! applied,
                             State + Cmd
                     end, 0),

    {{1, 1}, Leader} = ra:command(APid, 5),
    ct:pal("basic test leader ~p~n", [Leader]),
    waitfor(applied, apply_timeout),
    waitfor(applied, apply_timeout2),
    waitfor(applied, apply_timeout3),
    ok.


waitfor(Msg, ExitWith) ->
    receive
        Msg -> ok
    after 3000 ->
              exit(ExitWith)
    end.
