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

-include("ra.hrl").

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

-spec register_name(ra_uid(), file:filename(), atom()) -> yes | no.
register_name(UId, Pid, RaNodeName) ->
    true = ets:insert(?MODULE, {UId, Pid, RaNodeName}),
    true = ets:insert(?REVERSE_TBL, {RaNodeName, UId}),
    yes.

-spec unregister_name(ra_uid()) -> atom().
unregister_name(UId) ->
    [{_, _, NodeName}] = ets:take(?MODULE, UId),
    true = ets:delete(?REVERSE_TBL, NodeName),
    UId.

-spec whereis_name(ra_uid()) -> pid() | undefined.
whereis_name(UId) ->
    case ets:lookup(?MODULE, UId) of
        [{_Name, Pid, _RaNodeName}] -> Pid;
        [] -> undefined
    end.

-spec what_node(ra_uid()) -> atom().
what_node(UId) ->
    case ets:lookup(?MODULE, UId) of
        [{_UId, _Pid, Node}] -> Node;
        [] -> undefined
    end.

registered_name_from_node_name(NodeName) when is_atom(NodeName) ->
    case ets:lookup(?REVERSE_TBL, NodeName) of
        [] -> undefined;
        [{_, UId}] ->
            UId
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
