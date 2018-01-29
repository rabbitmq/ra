-module(ra_directory).

-export([
         init/0,
         register_name/3,
         unregister_name/1,
         whereis_name/1,
         what_node/1,
         registered_name_from_node_name/1,
         send/2
         ]).

-export_type([
              ]).

-define(REVERSE_TBL, ra_directory_reverse).

% registry for a ra node's locally unique name

-spec init() -> ok.
init() ->
    _ = ets:new(?MODULE, [named_table,
                          public,
                          {read_concurrency, true},
                          {write_concurrency, true}
                         ]),
    _ = ets:new(?REVERSE_TBL, [named_table,
                               public,
                               {read_concurrency, true},
                               {write_concurrency, true}
                              ]),
    ok.

-spec register_name(binary(), file:filename(), atom()) -> yes | no.
register_name(Name, Pid, RaNodeName) ->
    true = ets:insert(?MODULE, {Name, Pid, RaNodeName}),
    true = ets:insert(?REVERSE_TBL, {RaNodeName, Name}),
    yes.

-spec unregister_name(binary()) -> atom().
unregister_name(Name) ->
    [{_, _, NodeName}] = ets:take(?MODULE, Name),
    true = ets:delete(?REVERSE_TBL, NodeName),
    Name.

-spec whereis_name(binary()) -> pid() | undefined.
whereis_name(Name) ->
    case ets:lookup(?MODULE, Name) of
        [{_Name, Pid, _RaNodeName}] -> Pid;
        [] -> undefined
    end.

-spec what_node(binary()) -> atom().
what_node(Name) ->
    case ets:lookup(?MODULE, Name) of
        [{_Name, _Pid, Node}] -> Node;
        [] -> undefined
    end.

registered_name_from_node_name(NodeName) when is_atom(NodeName) ->
    case ets:lookup(?REVERSE_TBL, NodeName) of
        [] -> undefined;
        [{_, Name}] ->
            Name
    end.

-spec send(binary(), term()) -> pid().
send(Name, Msg) ->
    case whereis_name(Name) of
        undefined ->
            exit({badarg, {Name, Msg}});
        Pid ->
            erlang:send(Pid, Msg)
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
    ok = init(),
    Name = <<"test1">>,
    Self = self(),
    yes = register_name(Name, Self, test1),
    % registrations should always succeed - no negative test
    % no = register_name(Name, spawn(fun() -> ok end), test1),
    Self = whereis_name(Name),
    Name = registered_name_from_node_name(test1),
    test1 = what_node(Name),
    hi_Name = send(Name, hi_Name),
    receive
        hi_Name -> ok
    after 100 ->
              exit(await_msg_timeout)
    end,
    Name = unregister_name(Name),
    undefined = whereis_name(Name),
    undefined = what_node(Name),
    undefined = registered_name_from_node_name(test1),
    ok.

-endif.
