%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
         list_registered/1
         ]).

-export_type([
              ]).

-include("ra.hrl").

-define(REVERSE_TBL, ra_directory_reverse).

% registry for a ra servers's locally unique name

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
                          {read_concurrency, true},
                          {write_concurrency, true}
                         ]),
    ok = ra_lib:make_dir(Dir),
    Dets = filename:join(Dir, "names.dets"),
    {ok, NameRev} = dets:open_file(NameRev,
                                   [{file, Dets},
                                    {auto_save, 500},
                                    {access, read_write}]),
    ok.

-spec deinit(atom() | ra_system:names()) -> ok.
deinit(System) when is_atom(System) ->
    deinit(get_names(System));
deinit(#{directory := Name,
         directory_rev := NameRev}) ->
    _ = ets:delete(Name),
    _ = dets:close(NameRev),
    ok.

-spec register_name(ra_system:names() | atom(), ra_uid(), pid(), maybe(pid()), atom(),
                    ra_cluster_name()) -> ok.
register_name(System, UId, Pid, ParentPid, ServerName, ClusterName)
  when is_atom(System) ->
    register_name(get_names(System), UId, Pid,
                  ParentPid, ServerName, ClusterName);
register_name(#{
                directory := Directory,
                directory_rev := DirRev} = System, UId, Pid, ParentPid,
              ServerName, ClusterName) ->
    true = ets:insert(Directory, {UId, Pid, ParentPid, ServerName,
                                  ClusterName}),
    case uid_of(System, ServerName) of
        undefined ->
            ok = dets:insert(DirRev, {ServerName, UId});
        _ ->
            %% no need to insert into dets table if already there
            ok
    end.

-spec unregister_name(atom() | ra_system:names(), ra_uid()) -> ra_uid().
unregister_name(System, UId) when is_atom(System) ->
    unregister_name(get_names(System), UId);
unregister_name(#{directory := Directory,
                  directory_rev := DirRev}, UId) ->
    case ets:take(Directory, UId) of
        [{_, _, _, ServerName, _}] ->
            _ = ets:take(Directory, UId),
            ok = dets:delete(DirRev, ServerName),
            UId;
        [] ->
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
        [{_, Pid, _, _, _}] -> Pid;
        [] -> undefined
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
        [{_, _, Pid, _, _}] -> Pid;
        [] -> undefined
    end.

-spec name_of(atom() | ra_system:names(), ra_uid()) -> maybe(atom()).
name_of(SystemOrNames, UId) ->
    Tbl = get_name(SystemOrNames),
    case ets:lookup(Tbl, UId) of
        [{_, _, _, ServerName, _}] -> ServerName;
        [] -> undefined
    end.

-spec cluster_name_of(ra_system:names() | atom(), ra_uid()) ->
    maybe(ra_cluster_name()).
cluster_name_of(SystemOrNames, UId) ->
    Tbl = get_name(SystemOrNames),
    case ets:lookup(Tbl, UId) of
	[{_, _, _, _, ClusterName}] -> ClusterName;
	[] -> undefined
    end.


-spec pid_of(atom() | ra_system:names(), ra_uid()) -> maybe(pid()).
pid_of(SystemOrNames, UId) ->
    case ets:lookup(get_name(SystemOrNames), UId) of
        [{_, Pid, _, _, _}] -> Pid;
        [] -> undefined
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
    States = maps:from_list(ets:tab2list(ra_state)),
    Snaps = maps:from_list(ets:tab2list(ra_log_snapshot_state)),
    lists:foldl(fun ({UId, Pid, Parent, ServerName, ClusterName}, Acc) ->
                        Acc#{ServerName =>
                             #{uid => UId,
                               pid => Pid,
                               parent => Parent,
                               state => maps:get(ServerName, States, undefined),
                               cluster_name => ClusterName,
                               snapshot_state => maps:get(UId, Snaps,
                                                          undefined)}}
                end, #{}, Dir).

-spec list_registered(atom()) -> [{atom(), ra_uid()}].
list_registered(System) when is_atom(System) ->
    Tbl = get_reverse(System),
    dets:select(Tbl, [{'_', [], ['$_']}]).

get_name(#{directory := Tbl}) ->
    Tbl;
get_name(System) when is_atom(System) ->
    {ok, Tbl} = ra_system:lookup_name(System, directory),
    Tbl.

get_reverse(System) when is_atom(System) ->
    {ok, Tbl} = ra_system:lookup_name(System, directory_rev),
    Tbl.

get_names(System) when is_atom(System) ->
    #{names := Names} = ra_system:fetch(System),
    Names.

