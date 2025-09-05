%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-module(ra_directory).

-export([
         init/1,
         init/2,
         deinit/1,
         register_name/6,
         unregister_name/2,
         where_is/2,
         where_is_parent/2,
         name_of/2,
         cluster_name_of/2,
         pid_of/2,
         uid_of/2,
         overview/1,
         list_registered/1,
         is_registered_uid/2
         ]).

-export_type([
              ]).

-include("ra.hrl").

% registry for ra servers' locally unique name

-spec init(atom()) -> ok | {error, system_not_started}.
init(System) when is_atom(System) ->
    case ra_system:fetch(System) of
        undefined ->
            {error, system_not_started};
        #{data_dir := Dir,
          names := Names} ->
            init(Dir, Names)
    end.

-spec init(file:filename(), ra_system:names()) -> ok.
init(Dir, #{directory := Name,
            directory_rev := NameRev}) ->
    _ = ets:new(Name, [named_table,
                       public,
                       {read_concurrency, true}
                      ]),
    ok = ra_lib:make_dir(Dir),
    Dets = filename:join(Dir, "names.dets"),
    {ok, NameRev} = dets:open_file(NameRev,
                                   [{file, Dets},
                                    {auto_save, 500},
                                    {access, read_write}]),
    _ = dets:foldl(fun({ServerName, UId}, Acc) ->
                           true = ets:insert(Name, {UId, undefined, undefined,
                                                    ServerName, undefined}),
                           Acc
                   end, [], NameRev),
    ok.

-spec deinit(atom() | ra_system:names()) -> ok.
deinit(System) when is_atom(System) ->
    deinit(get_names(System));
deinit(#{directory := Name,
         directory_rev := NameRev}) ->
    _ = ets:delete(Name),
    ok = dets:sync(NameRev),
    _ = dets:close(NameRev),
    ok.

-spec register_name(ra_system:names() | atom(), ra_uid(), pid(), option(pid()), atom(),
                    ra_cluster_name()) -> ok.
register_name(System, UId, Pid, ParentPid, ServerName, ClusterName)
  when is_atom(System) ->
    register_name(get_names(System), UId, Pid,
                  ParentPid, ServerName, ClusterName);
register_name(#{directory := Directory,
                directory_rev := DirRev} = System, UId, Pid, ParentPid,
              ServerName, ClusterName) ->
    true = ets:insert(Directory, {UId, Pid, ParentPid, ServerName,
                                  ClusterName}),
    case uid_of(System, ServerName) of
        undefined ->
            ok = dets:insert(DirRev, {ServerName, UId});
        UId ->
            %% no need to insert into dets table if already there
            ok;
        OtherUId ->
            ok = dets:insert(DirRev, {ServerName, UId}),
            ?WARN("ra server with name ~ts UId ~s replaces prior UId ~s",
                  [ServerName, UId, OtherUId]),
            ok
    end.

-spec unregister_name(atom() | ra_system:names(), ra_uid()) -> ra_uid().
unregister_name(System, UId) when is_atom(System) ->
    unregister_name(get_names(System), UId);
unregister_name(#{directory := Directory,
                  directory_rev := DirRev}, UId) ->
    case ets:take(Directory, UId) of
        [{_, _, _, ServerName, _}] ->
            ok = dets:delete(DirRev, ServerName),
            UId;
        [] ->
            _ = dets:select_delete(DirRev, [{{'_', UId}, [], [true]}]),
            UId
    end.

-spec where_is(atom() | ra_system:names(), ra_uid() | atom()) ->
    pid() | undefined.
where_is(System, ServerName) when is_atom(System) ->
    where_is(get_names(System), ServerName);
where_is(#{directory_rev := DirRev} = Names, ServerName)
  when is_atom(ServerName) ->
    case dets:lookup(DirRev, ServerName) of
        [] -> undefined;
        [{_, UId}] ->
            where_is(Names, UId)
    end;
where_is(#{directory := Dir}, UId) when is_binary(UId) ->
    case ets:lookup(Dir, UId) of
        [{_, Pid, _, _, _}] when is_pid(Pid) ->
            Pid;
        _ -> undefined
    end.

-spec where_is_parent(atom() | ra_system:names(), ra_uid() | atom()) ->
    pid() | undefined.
where_is_parent(System, ServerName) when is_atom(System) ->
    where_is_parent(get_names(System), ServerName);
where_is_parent(#{directory_rev := DirRev} = Names, ServerName)
  when is_atom(ServerName) ->
    case dets:lookup(DirRev, ServerName) of
        [] -> undefined;
        [{_, UId}] ->
            where_is_parent(Names, UId)
    end;
where_is_parent(#{directory := Dir}, UId) when is_binary(UId) ->
    case ets:lookup(Dir, UId) of
        [{_, _, Pid, _, _}] when is_pid(Pid) -> Pid;
        _ -> undefined
    end.

-spec name_of(atom() | ra_system:names(), ra_uid()) -> option(atom()).
name_of(SystemOrNames, UId) ->
    Tbl = get_tbl(SystemOrNames),
    case ets:lookup(Tbl, UId) of
        [{_, _, _, ServerName, _}] -> ServerName;
        [] ->
            undefined
    end.

-spec cluster_name_of(ra_system:names() | atom(), ra_uid()) ->
    option(ra_cluster_name()).
cluster_name_of(SystemOrNames, UId) ->
    Tbl = get_tbl(SystemOrNames),
    case ets:lookup(Tbl, UId) of
        [{_, _, _, _, ClusterName}]
          when ClusterName /= undefined ->
            ClusterName;
        _ -> undefined
    end.


-spec pid_of(atom() | ra_system:names(), ra_uid()) -> option(pid()).
pid_of(SystemOrNames, UId) ->
    case ets:lookup(get_tbl(SystemOrNames), UId) of
        [{_, Pid, _, _, _}] when is_pid(Pid) -> Pid;
        _ -> undefined
    end.

uid_of(System, ServerName) when is_atom(System) ->
    uid_of(get_names(System), ServerName);
uid_of(#{directory_rev := Tbl}, ServerName) when is_atom(ServerName) ->
    case dets:lookup(Tbl, ServerName) of
        [] -> undefined;
        [{_, UId}] ->
            UId
    end;
uid_of(SystemOrNames, {ServerName, _}) when is_atom(ServerName) ->
    uid_of(SystemOrNames, ServerName).

overview(System) when is_atom(System) ->
    #{directory := Tbl,
      directory_rev := _TblRev} = get_names(System),
    Dir = ets:tab2list(Tbl),
    Rows = lists:map(fun({K, S, V}) ->
                             {K, {S, V}}
                     end,
                     ets:tab2list(ra_state)),
    States = maps:from_list(Rows),
    Snaps = lists:foldl(
              fun (T, Acc) ->
                      Acc#{element(1, T) => erlang:delete_element(1, T)}
              end, #{}, ets:tab2list(ra_log_snapshot_state)),
    lists:foldl(fun ({UId, Pid, Parent, ServerName, ClusterName}, Acc) ->
                        {S, V} = maps:get(ServerName, States, {undefined, undefined}),
                        Acc#{ServerName =>
                             #{uid => UId,
                               pid => Pid,
                               parent => Parent,
                               state => S,
                               membership => V,
                               cluster_name => ClusterName,
                               snapshot_state => maps:get(UId, Snaps,
                                                          undefined)}}
                end, #{}, Dir).

-spec list_registered(atom() | ra_system:names()) ->
    [{atom(), ra_uid()}].
list_registered(SystemOrNames)
  when is_atom(SystemOrNames) orelse
       is_map(SystemOrNames) ->
    Tbl = get_reverse(SystemOrNames),
    dets:select(Tbl, [{'_', [], ['$_']}]).

-spec is_registered_uid(atom() | ra_system:names(), ra_uid()) -> boolean().
is_registered_uid(SystemOrNames, UId)
  when (is_atom(SystemOrNames) orelse is_map(SystemOrNames)) andalso
       is_binary(UId) ->
    name_of(SystemOrNames, UId) =/= undefined.

get_tbl(#{directory := Tbl}) ->
    Tbl;
get_tbl(System) when is_atom(System) ->
    {ok, Tbl} = ra_system:lookup_name(System, directory),
    Tbl.

get_reverse(System) when is_atom(System) ->
    {ok, Tbl} = ra_system:lookup_name(System, directory_rev),
    Tbl;
get_reverse(#{directory_rev := DirRev}) ->
    DirRev.

get_names(System) when is_atom(System) ->
    #{names := Names} = ra_system:fetch(System),
    Names.
