-module(ra_directory).

-export([
         init/1,
         deinit/0,
         register_name/3,
         register_name/4,
         register_name/5,
         unregister_name/1,
         where_is/1,
         where_is_parent/1,
         name_of/1,
         cluster_name_of/1,
         pid_of/1,
         uid_of/1,
         send/2,
         overview/0,
         list_registered/0
         ]).

-export_type([
              ]).

-include("ra.hrl").

-define(REVERSE_TBL, ra_directory_reverse).

% registry for a ra servers's locally unique name

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
                                         {auto_save, 500},
                                         {access, read_write}]),
    ok.

-spec deinit() -> ok.
deinit() ->
    _ = ets:delete(?MODULE),
    _ = dets:close(?REVERSE_TBL),
    ok.

-spec register_name(ra_uid(), pid(), maybe(pid()), atom(),
		    ra_cluster_name()) -> yes | no.
register_name(UId, Pid, ParentPid, RaServerName, ClusterName) ->
    true = ets:insert(?MODULE, {UId, Pid, ParentPid, RaServerName,
                                ClusterName}),
    ok = dets:insert(?REVERSE_TBL, {RaServerName, UId}),
    yes.

-spec register_name(ra_uid(), pid(), maybe(pid()), atom()) -> yes | no.
register_name(UId, Pid, ParentPid, RaServerName) ->
    register_name(UId, Pid, ParentPid, RaServerName, undefined).

-spec register_name(ra_uid(), pid(), atom()) -> yes | no.
register_name(UId, Pid, RaServerName) ->
    register_name(UId, Pid, undefined, RaServerName).

-spec unregister_name(ra_uid()) -> ra_uid().
unregister_name(UId) ->
    case ets:take(?MODULE, UId) of
        [{_, _, _, ServerName, _}] ->
            _ = ets:take(?MODULE, UId),
            ok = dets:delete(?REVERSE_TBL, ServerName),
            UId;
        [] ->
            UId
    end.

-spec where_is(ra_uid() | atom()) -> pid() | undefined.
where_is(ServerName) when is_atom(ServerName) ->
    case dets:lookup(?REVERSE_TBL, ServerName) of
        [] -> undefined;
        [{_, UId}] ->
            where_is(UId)
    end;
where_is(UId) when is_binary(UId) ->
    case ets:lookup(?MODULE, UId) of
        [{_, Pid, _, _, _}] -> Pid;
        [] -> undefined
    end.

-spec where_is_parent(ra_uid() | atom()) -> pid() | undefined.
where_is_parent(ServerName) when is_atom(ServerName) ->
    case dets:lookup(?REVERSE_TBL, ServerName) of
        [] -> undefined;
        [{_, UId}] ->
            where_is_parent(UId)
    end;
where_is_parent(UId) when is_binary(UId) ->
    case ets:lookup(?MODULE, UId) of
        [{_, _, Pid, _, _}] -> Pid;
        [] -> undefined
    end.

-spec name_of(ra_uid()) -> maybe(atom()).
name_of(UId) ->
    case ets:lookup(?MODULE, UId) of
        [{_, _, _, Node, _}] -> Node;
        [] -> undefined
    end.

-spec cluster_name_of(ra_uid()) -> maybe(ra_cluster_name()).
cluster_name_of(UId) ->
    case ets:lookup(?MODULE, UId) of
	[{_, _, _, _, ClusterName}] -> ClusterName;
	[] -> undefined
    end.


-spec pid_of(ra_uid()) -> maybe(pid()).
pid_of(UId) ->
    case ets:lookup(?MODULE, UId) of
        [{_, Pid, _, _, _}] -> Pid;
        [] -> undefined
    end.

uid_of(ServerName) when is_atom(ServerName) ->
    case dets:lookup(?REVERSE_TBL, ServerName) of
        [] -> undefined;
        [{_, UId}] ->
            UId
    end.

-spec send(ra_uid() | atom(), term()) -> pid().
send(UIdOrName, Msg) ->
    case where_is(UIdOrName) of
        undefined ->
            exit({badarg, {UIdOrName, Msg}});
        Pid ->
            _ = erlang:send(Pid, Msg),
            Pid
    end.

overview() ->
    Dir = ets:tab2list(?MODULE),
    States = maps:from_list(ets:tab2list(ra_state)),
    Snaps = maps:from_list(ets:tab2list(ra_log_snapshot_state)),
    lists:foldl(fun ({UId, Pid, Parent, Node, ClusterName}, Acc) ->
                        Acc#{Node =>
                             #{uid => UId,
                               pid => Pid,
                               parent => Parent,
                               state => maps:get(Node, States, undefined),
                               cluster_name => ClusterName,
                               snapshot_state => maps:get(UId, Snaps,
                                                          undefined)}}
                end, #{}, Dir).

-spec list_registered() -> [{atom(), ra_uid()}].
list_registered() ->
    dets:select(?REVERSE_TBL, [{'_', [], ['$_']}]).
