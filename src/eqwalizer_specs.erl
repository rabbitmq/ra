-module(eqwalizer_specs).
-compile(warn_missing_spec).
% elp:ignore W0054 (no_nowarn_suppressions)
-compile([export_all, nowarn_export_all]).


-spec 'erpc:call'(node(), module(), atom(), [term()]) -> dynamic().
'erpc:call'(_, _, _, _) -> error(eqwalizer_specs).

-spec 'lists:foldl'(fun((T, Acc) -> Acc), Acc, [T]) -> Acc.
'lists:foldl'(_, _, _) -> error(eqwalizer_specs).

-spec 'lists:foldr'(fun((T, Acc) -> Acc), Acc, [T]) -> Acc.
'lists:foldr'(_, _, _) -> error(eqwalizer_specs).

 -spec 'lists:nth'(pos_integer(), [T]) -> T.
'lists:nth'(_, _) -> error(eqwalizer_specs).

 -spec 'lists:max'([T]) -> T.
'lists:max'(_) -> error(eqwalizer_specs).

 -spec 'lists:sort'([T]) -> [T].
'lists:sort'(_) -> error(eqwalizer_specs).

 -spec 'lists:usort'([T]) -> [T].
'lists:usort'(_) -> error(eqwalizer_specs).

 -spec 'lists:delete'(T, [T]) -> [T].
'lists:delete'(_, _) -> error(eqwalizer_specs).

-spec 'lists:sort'(fun((T, T) -> boolean()), [T]) -> [T].
'lists:sort'(_, _) -> error(eqwalizer_specs).

-spec 'lists:splitwith'(fun((T) -> boolean()), [T]) -> [T].
'lists:splitwith'(_, _) -> error(eqwalizer_specs).

-spec 'lists:foreach'(fun((T) -> ok), [T]) -> [T].
'lists:foreach'(_, _) -> error(eqwalizer_specs).

 -spec 'lists:reverse'([T]) -> [T].
'lists:reverse'(_) -> error(eqwalizer_specs).

 -spec 'lists:reverse'([T], [T]) -> [T].
'lists:reverse'(_, _) -> error(eqwalizer_specs).

 -spec 'erlang:hd'([A, ...]) -> A.
'erlang:hd'(_) -> error(eqwalizer_specs).

-spec 'erlang:max'(A, B) -> A | B.
'erlang:max'(_, _) -> error(eqwalizer_specs).

-spec 'erlang:min'(A, B) -> A | B.
'erlang:min'(_, _) -> error(eqwalizer_specs).

-spec 'erlang:system_time'() -> pos_integer().
'erlang:system_time'() -> error(eqwalizer_specs).

-spec 'erlang:system_time'(erlang:time_unit()) -> pos_integer().
'erlang:system_time'(_) -> error(eqwalizer_specs).

%% -------- gen_statem --------

-spec 'gen_statem:call'(gen_statem:server_ref(), term()) -> dynamic().
'gen_statem:call'(_, _) -> error(eqwalizer_specs).

-spec 'gen_statem:call'(gen_statem:server_ref(), term(), Timeout) -> dynamic() when
    Timeout :: timeout() | {clean_timeout, timeout()} | {dirty_timeout, timeout()}.
'gen_statem:call'(_, _, _) -> error(eqwalizer_specs).

-spec 'gen_statem:receive_response'(gen_statem:request_id(), Timeout) ->
     dynamic() when
    Timeout :: timeout() |
    {clean_timeout, timeout()} |
    {dirty_timeout, timeout()}.
'gen_statem:receive_response'(_, _) -> error(eqwalizer_specs).

 %% -------- application --------

-spec 'application:get_all_env'(App :: atom()) -> [{atom(), dynamic()}].
'application:get_all_env'(_) -> error(eqwalizer_specs).

-spec 'application:get_env'(Param :: atom()) -> undefined | {ok, dynamic()}.
'application:get_env'(_) -> error(eqwalizer_specs).

-spec 'application:get_env'(App :: atom(), Param :: atom()) ->
    undefined | {ok, dynamic()}.
'application:get_env'(_, _) -> error(eqwalizer_specs).

-spec 'application:get_env'(App :: atom(), Param :: atom(), Default :: term()) ->
    dynamic().
'application:get_env'(_, _, _) -> error(eqwalizer_specs).

-spec 'application:get_key'(Key :: atom()) -> undefined | {ok, dynamic()}.
'application:get_key'(_) -> error(eqwalizer_specs).

-spec 'application:get_key'(App :: atom(), Key :: atom()) -> undefined | {ok, dynamic()}.
'application:get_key'(_, _) -> error(eqwalizer_specs).

-spec 'erlang:binary_to_term'(binary()) -> dynamic().
'erlang:binary_to_term'(_) -> error(eqwalizer_specs).

 -spec 'ets:info'
    (ets:table(), compressed | decentralized_counters | fixed | named_table | read_concurrency | write_concurrency) ->
        boolean();
    (ets:table(), binary) -> list();
    (ets:table(), heir) -> pid() | none;
    (ets:table(), id) -> ets:tid();
    (ets:table(), keypos | memory | size) -> non_neg_integer();
    (ets:table(), name) -> atom();
    (ets:table(), node) -> node();
    (ets:table(), owner) -> pid();
    (ets:table(), safe_fixed | safe_fixed_monotonic_time) -> tuple() | false;
    (ets:table(), stats) -> tuple();
    (ets:table(), protection) -> ets:table_access();
    (ets:table(), type) -> ets:table_type().
'ets:info'(_, _) -> error(eqwalizer_specs).

%% -------- gen_server --------

-spec 'gen_server:call'(gen_server:server_ref(), term()) -> dynamic().
'gen_server:call'(_, _) -> error(eqwalizer_specs).

-spec 'gen_server:call'(gen_server:server_ref(), term(), timeout()) -> dynamic().
'gen_server:call'(_, _, _) -> error(eqwalizer_specs).
