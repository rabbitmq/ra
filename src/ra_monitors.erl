%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(ra_monitors).

-include("ra.hrl").
-export([
         init/0,
         add/3,
         remove/3,
         remove_all/2,
         handle_down/2,
         components/2
         ]).

%% holds static or rarely changing fields

-type component() :: machine | aux | snapshot_sender.

-opaque state() :: #{pid() => {reference(), #{component() => ok}},
                     node() => #{component() => ok}}.

-export_type([
              state/0
             ]).

-spec init() -> state().
init() ->
    #{}.

-spec add(pid() | node(), component(), state()) -> state().
add(Pid, Component, Monitors) when is_pid(Pid) ->
    case Monitors of
        #{Pid := {MRef, Components}} ->
            Monitors#{Pid => {MRef, Components#{Component => ok}}};
        _ ->
            MRef = erlang:monitor(process, Pid),
            Monitors#{Pid => {MRef, #{Component => ok}}}
    end;
add(Node, Component, Monitors) when is_atom(Node) ->
    case Monitors of
        #{Node := Components} ->
            case maps:is_key(Component, Components) of
                false ->
                    emit_current_node_state(Node);
                true ->
                    ok
            end,
            Monitors#{Node => Components#{Component => ok}};
        _ ->
            emit_current_node_state(Node),
            Monitors#{Node => #{Component => ok}}
    end.

emit_current_node_state(Node) ->
    Nodes = [node() | nodes()],
    %% fake event for newly registered component
    %% so that it discovers the current node state
    case lists:member(Node, Nodes) of
        true ->
            self() ! {nodeup, Node, []},
            ok;
        false ->
            self() ! {nodedown, Node, []},
            ok
    end.


-spec remove(pid() | node(), component(), state()) -> state().
remove(Target, Component, Monitors) ->
    case Monitors of
        #{Target := {MRef, #{Component := _} = Components0}} ->
            case maps:remove(Component, Components0) of
                Components when map_size(Components) == 0 ->
                    %% if pid, demonitor using ref
                    true = erlang:demonitor(MRef),
                    maps:remove(Target, Monitors);
                Components ->
                    Monitors#{Target => Components}
            end;
        #{Target := #{Component := _} = Components0} ->
            case maps:remove(Component, Components0) of
                Components when map_size(Components) == 0 ->
                    maps:remove(Target, Monitors);
                Components ->
                    Monitors#{Target => Components}
            end;
        _ ->
            Monitors
    end.

-spec remove_all(component(), state()) -> state().
remove_all(Component, Monitors) ->
    lists:foldl(fun(T, Acc) ->
                        remove(T, Component, Acc)
                end, Monitors, maps:keys(Monitors)).

-spec handle_down(pid() | node(), state()) ->
    {[component()], state()}.
handle_down(Target, Monitors0)
  when is_pid(Target) orelse is_atom(Target) ->
    case maps:take(Target, Monitors0) of
        {{_MRef, CompsMap}, Monitors} ->
            {maps:keys(CompsMap), Monitors};
        {CompsMap, _Monitors} when is_map(CompsMap) ->
            %% nodes aren't removed when down
            {maps:keys(CompsMap), Monitors0};
        error ->
            {[], Monitors0}
    end;
handle_down(Target, Monitors0) ->
    ?DEBUG("ra_monitors: target ~w not recognised", [Target]),
    {[], Monitors0}.


-spec components(pid() | node(), state()) -> [component()].
components(Target, Monitors) ->
    case maps:get(Target, Monitors, error) of
        {_MRef, CompsMap} ->
            maps:keys(CompsMap);
        CompsMap when is_map(CompsMap) ->
            maps:keys(CompsMap);
        error ->
            []
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basics_test() ->
    M0 = init(),
    M1 = add(self(), machine, M0),
    [machine] = components(self(), M1),
    M2 = add(self(), aux, M1),
    [aux, machine] = components(self(), M2),
    M3 = remove(self(), machine, M2),
    [aux] = components(self(), M3),
    {[aux, machine], M5} = handle_down(self(), M2),
    [] = components(self(), M5),
    ok.

nodes_test() ->
    M0 = init(),
    M1 = add(fake@blah, machine, M0),
    receive
        {nodedown, fake@blah, _} -> ok
    after 100 -> exit(nodedown_timeout)
    end,
    M2 = add(fake2@blah, machine, M1),
    receive
        {nodedown, fake2@blah, _} -> ok
    after 100 -> exit(nodedown_timeout_2)
    end,
    M3 = add(fake2@blah, aux, M2),
    receive
        {nodedown, fake2@blah, _} -> ok
    after 100 -> exit(nodedown_timeout_3)
    end,
    M4 = remove_all(machine, M3),
    ?assertEqual([fake2@blah], maps:keys(M4)),
    ?assertEqual([#{aux => ok}], maps:values(M4)),
    ok.


-endif.
