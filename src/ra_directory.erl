-module(ra_directory).

-export([
         init/1,
         deinit/0,
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

-spec init(file:filename()) -> ok.
init(Dir) ->
    _ = ets:new(?MODULE, [named_table,
                          public,
                          {read_concurrency, true},
                          {write_concurrency, true}
                         ]),
    Dets = filename:join(Dir, "names.dets"),
    ok = filelib:ensure_dir(Dets),
    {ok, ?REVERSE_TBL} = dets:open_file(?REVERSE_TBL,
                                        [{file, Dets},
                                         {auto_save, 5000},
                                         {access, read_write}]),
    ok.

-spec deinit() -> ok.
deinit() ->
    _ = ets:delete(?MODULE),
    _ = dets:close(?REVERSE_TBL),
    ok.

-spec register_name(ra_uid(), file:filename(), atom()) -> yes | no.
register_name(UId, Pid, RaNodeName) ->
    true = ets:insert(?MODULE, {UId, Pid, RaNodeName}),
    ok = dets:insert(?REVERSE_TBL, {RaNodeName, UId}),
    yes.

-spec unregister_name(ra_uid()) -> ra_uid().
unregister_name(UId) ->
    case ets:take(?MODULE, UId) of
        [{_, _, NodeName}] ->
            ets:take(?MODULE, UId),
            ok = dets:delete(?REVERSE_TBL, NodeName),
            UId;
        [] ->
            UId
    end.

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
    case dets:lookup(?REVERSE_TBL, NodeName) of
        [] -> undefined;
        [{_, UId}] ->
            UId
    end.

-spec send(ra_uid(), term()) -> pid().
send(Name, Msg) ->
    case whereis_name(Name) of
        undefined ->
            exit({badarg, {Name, Msg}});
        Pid ->
            _ = erlang:send(Pid, Msg),
            Pid
    end.
