-module(eqwalizer_specs). -compile(warn_missing_spec).
% elp:ignore W0054 (no_nowarn_suppressions)
-compile([export_all, nowarn_export_all]).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'erpc:call'/4}).
-spec 'erpc:call'(node(), module(), atom(), [term()]) -> dynamic().
'erpc:call'(_, _, _, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'lists:foldl'/3}).
-spec 'lists:foldl'(fun((T, Acc) -> Acc), Acc, [T]) -> Acc.
'lists:foldl'(_, _, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'lists:foldr'/3}).
-spec 'lists:foldr'(fun((T, Acc) -> Acc), Acc, [T]) -> Acc.
'lists:foldr'(_, _, _) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'lists:nth'/2}).
 -spec 'lists:nth'(pos_integer(), [T]) -> T.
'lists:nth'(_, _) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'lists:unzip'/1}).
 -spec 'lists:unzip'([{A, B}]) -> {[A], [B]}.
'lists:unzip'(_) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'lists:max'/1}).
 -spec 'lists:max'([T]) -> T.
'lists:max'(_) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'lists:append'/1}).
 -spec 'lists:append'([T | [T]]) -> [T].
'lists:append'(_) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'lists:last'/1}).
 -spec 'lists:last'([T]) -> T.
'lists:last'(_) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'lists:sort'/1}).
 -spec 'lists:sort'([T]) -> [T].
'lists:sort'(_) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'lists:usort'/1}).
 -spec 'lists:usort'([T]) -> [T].
'lists:usort'(_) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'lists:delete'/2}).
 -spec 'lists:delete'(T, [T]) -> [T].
'lists:delete'(_, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'lists:sort'/2}).
-spec 'lists:sort'(fun((T, T) -> boolean()), [T]) -> [T].
'lists:sort'(_, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'lists:splitwith'/2}).
-spec 'lists:splitwith'(fun((T) -> boolean()), [T]) -> [T].
'lists:splitwith'(_, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'lists:foreach'/2}).
-spec 'lists:foreach'(fun((T) -> ok), [T]) -> [T].
'lists:foreach'(_, _) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'lists:reverse'/1}).
 -spec 'lists:reverse'([T]) -> [T].
'lists:reverse'(_) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'lists:reverse'/2}).
 -spec 'lists:reverse'([T], [T]) -> [T].
'lists:reverse'(_, _) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'erlang:hd'/1}).
 -spec 'erlang:hd'([A, ...]) -> A.
'erlang:hd'(_) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'erlang:max'/2}).
-spec 'erlang:max'(A, B) -> A | B.
'erlang:max'(_, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'erlang:min'/2}).
-spec 'erlang:min'(A, B) -> A | B.
'erlang:min'(_, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'erlang:system_time'/0}).
-spec 'erlang:system_time'() -> pos_integer().
'erlang:system_time'() -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'erlang:system_time'/1}).
-spec 'erlang:system_time'(erlang:time_unit()) -> pos_integer().
'erlang:system_time'(_) -> error(eqwalizer_specs).

%% -------- gen_statem --------

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'gen_statem:call'/2}).
-spec 'gen_statem:call'(gen_statem:server_ref(), term()) -> dynamic().
'gen_statem:call'(_, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'gen_statem:call'/3}).
-spec 'gen_statem:call'(gen_statem:server_ref(), term(), Timeout) -> dynamic() when
    Timeout :: timeout() | {clean_timeout, timeout()} | {dirty_timeout, timeout()}.
'gen_statem:call'(_, _, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'gen_statem:receive_response'/2}).
-spec 'gen_statem:receive_response'(gen_statem:request_id(), Timeout) ->
     dynamic() when
    Timeout :: timeout() |
    {clean_timeout, timeout()} |
    {dirty_timeout, timeout()}.
'gen_statem:receive_response'(_, _) -> error(eqwalizer_specs).

 %% -------- application --------

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'application:get_all_env'/1}).
-spec 'application:get_all_env'(App :: atom()) -> [{atom(), dynamic()}].
'application:get_all_env'(_) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'application:get_env'/1}).
-spec 'application:get_env'(Param :: atom()) -> undefined | {ok, dynamic()}.
'application:get_env'(_) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'application:get_env'/2}).
-spec 'application:get_env'(App :: atom(), Param :: atom()) ->
    undefined | {ok, dynamic()}.
'application:get_env'(_, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'application:get_env'/3}).
-spec 'application:get_env'(App :: atom(), Param :: atom(), Default :: term()) ->
    dynamic().
'application:get_env'(_, _, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'application:get_key'/1}).
-spec 'application:get_key'(Key :: atom()) -> undefined | {ok, dynamic()}.
'application:get_key'(_) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'application:get_key'/2}).
-spec 'application:get_key'(App :: atom(), Key :: atom()) -> undefined | {ok, dynamic()}.
'application:get_key'(_, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'erlang:binary_to_term'/1}).
-spec 'erlang:binary_to_term'(binary()) -> dynamic().
'erlang:binary_to_term'(_) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'ets:info'/2}).
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

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'ets:select'/1}).
 -spec 'ets:select'(EtsContinuation) ->
     {[dynamic()], EtsContinuation} | '$end_of_table'
       when
       EtsContinuation :: dynamic().
'ets:select'(_) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'ets:select'/2}).
-spec 'ets:select'(ets:table(), ets:match_spec()) -> [dynamic()].
'ets:select'(_, _) -> error(eqwalizer_specs).

 % elp:ignore W0048 (no_dialyzer_attribute)
 -dialyzer({nowarn_function, 'dets:select'/2}).
-spec 'dets:select'(dets:tab_name(), ets:match_spec()) -> [dynamic()].
'dets:select'(_, _) -> error(eqwalizer_specs).

%% -------- gen__batch_server --------

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'gen_batch_server:call'/2}).
-spec 'gen_batch_server:call'(gen_batch_server:server_ref(), term()) -> dynamic().
'gen_batch_server:call'(_, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'gen_batch_server:call'/3}).
-spec 'gen_batch_server:call'(gen_batch_server:server_ref(), term(), timeout()) -> dynamic().
'gen_batch_server:call'(_, _, _) -> error(eqwalizer_specs).
%% -------- gen_server --------

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'gen_server:call'/2}).
-spec 'gen_server:call'(gen_server:server_ref(), term()) -> dynamic().
'gen_server:call'(_, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'gen_server:call'/3}).
-spec 'gen_server:call'(gen_server:server_ref(), term(), timeout()) -> dynamic().
'gen_server:call'(_, _, _) -> error(eqwalizer_specs).

% elp:ignore W0048 (no_dialyzer_attribute)
-dialyzer({nowarn_function, 'persistent_term:get'/2}).
-spec 'persistent_term:get'(dynamic(), dynamic()) -> dynamic().
'persistent_term:get'(_, _) -> error(eqwalizer_specs).
