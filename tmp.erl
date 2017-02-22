-module(tmp).


-compile(export_all).


cluster() ->
    raga:start_cluster(3, "test", fun(Cmd, State) -> State + Cmd end, 0).
