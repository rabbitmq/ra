-module(tmp).


-compile(export_all).


cluster() ->
    ra:start_cluster(3, "test", fun(Cmd, State) -> State + Cmd end, 0).
