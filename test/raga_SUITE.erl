-module(raga_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() ->
    [
     start_cluster
    ].

start_cluster(_Config) ->
    [{APid, _A}, {BPid, _B}, _C] = raga:start_cluster(
                             3, "test", fun(Cmd, State) -> State + Cmd end, 0),

    true = (undefined /= is_process_alive(APid)),
    true = (undefined /= is_process_alive(BPid)),
    ok.
