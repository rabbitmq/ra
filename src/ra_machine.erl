%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
%% @doc The `ra_machine' behaviour.
%%
%% Used to implement the logic for the state machine running inside Ra.
%%
%% == Callbacks ==
%%
%% <code>-callback init(Conf :: {@link machine_init_args()}) -> state()'</code>
%%
%% Initialize a new machine state.
%%
%% <br></br>
%% <code>-callback apply(Meta :: command_meta_data(),
%%                       {@link command()}, State) ->
%%    {State, {@link reply()}, {@link effects()}} | {State, {@link reply()}}</code>
%%
%% Applies each entry to the state machine.
%%
%% <br></br>
%% <code>
%% -callback state_enter(ra_server:ra_state() | eol, state()) -> effects().
%% </code>
%%
%% Optional. Called when the ra server enters a new state. Called for all states
%% in the ra_server_proc gen_statem implementation not just for the standard
%% Raft states (follower, candidate, leader). If implemented it is sensible
%% to include a catch all clause as new states may be implemented in the future.
%%
%%<br></br>
%% <code>-callback tick(TimeMs :: milliseconds(), state()) -> effects().</code>
%%
%%
%% Optional. Called periodically.
%% Suitable for issuing periodic side effects such as updating metrics systems.
%%
%%<br></br>
%% <code>-callback overview(state()) -> map(). </code>
%%
%% Optional. A map of overview information. Needs to be efficient.
%%
%%
%%<br></br>
%% <code>
%% -callback version() -> version().
%% </code>
%%
%% Optional: Returns the latest machine version. If not implemented this is
%% defaulted to 0.
%%<br></br>
%%
%% <code>
%% -callback which_module(version()) -> module().
%% </code>
%%
%% Optional: implements a lookup from version to the module implementing the
%% machine logic for that version.

-module(ra_machine).

-compile({no_auto_import, [apply/3]}).

-include("ra.hrl").

-export([init/3,
         apply/4,
         tick/3,
         snapshot_installed/5,
         state_enter/3,
         overview/2,
         live_indexes/2,
         query/3,
         module/1,
         init_aux/2,
         handle_aux/6,
         handle_aux/7,
         snapshot_module/1,
         version/1,
         which_module/2,
         which_aux_fun/1,
         is_versioned/1
        ]).

-type state() :: term().
%% The state for a given machine implementation.

-type user_command() :: term().
%% the command type for a given machine implementation

-type machine_init_args() :: #{name := atom(),
                               machine_version => version(),
                               atom() => term()}.
%% the configuration passed to the init callback

-type machine() :: {machine, module(), AddInitArgs :: #{term() => term()}}.
%% Machine configuration.
%% the `module()' should implement the {@link ra_machine} behaviour.

-type milliseconds() :: non_neg_integer().

-type builtin_command() :: {down, pid(), term()} |
                           {nodeup | nodedown, node()} |
                           {machine_version, From :: version(), To :: version()} |
                           {timeout, term()}.
%% These commands may be passed to the {@link apply/2} function in reaction
%% to monitor effects

-type send_msg_opt() :: ra_event | cast | local.
%% ra_event: the message will be wrapped up and sent as a ra event
%% e.g: `{ra_event, ra_server_id(), Msg}'
%%
%% cast: the message will be wrapped as a gen cast: ``{'$cast', Msg}''
%% local: the message will be sent by the local member if there is one
%% configured

-type send_msg_opts() :: send_msg_opt() | [send_msg_opt()].
-type locator() :: pid() | atom() | {atom(), node()}.

-type version() :: non_neg_integer().

-type effect() ::
    %% These effects are only executed on the leader
    {send_msg, To :: locator(), Msg :: term()} |
    {mod_call, module(), Function :: atom(), [term()]} |
    %% appends a user command to the raft log
    {append, Cmd :: term()} |
    {append, Cmd :: term(), ra_server:command_reply_mode()} |
    {monitor, process, pid()} |
    {monitor, node, node()} |
    {demonitor, process, pid()} |
    {demonitor, node, node()} |
    {timer, term(), non_neg_integer() | infinity} |
    {log, [ra_index()], fun(([user_command()]) -> effects())} |

    %% these are either conditional on the local configuration or
    %% will always be evaluated when seen by members in any raft state
    {send_msg, To :: locator(), Msg :: term(), Options :: send_msg_opts()} |
    {log, [ra_index()], fun(([user_command()]) -> effects()), {local, node()}} |
    {log_ext, [ra_index()], fun(([ra_log:read_plan()]) -> effects()), {local, node()}} |
    {release_cursor, ra_index(), state()} |
    {release_cursor, ra_index()} |
    {checkpoint, ra_index(), state()} |
    {aux, term()} |
    %% like append/3 but a special backwards compatible function
    %% that tries to execute in any raft state
    {try_append, term(), ra_server:command_reply_mode()} |
    garbage_collection.

%% Effects are data structures that can be returned by {@link apply/3} to ask
%% ra to realise a side-effect in the "real world", such as sending
%% a message to a process, monitoring a process, calling a function or
%% forcing a GC run.
%%
%% Although both leaders and followers will process the same commands, effects
%% are typically only applied on the leader. The only exceptions to this are:
%% <ul>
%% <li>`release_cursor'</li>
%% <li>`checkpoint'</li>
%% <li>`garbage_collect'</li>
%% </ul>
%% The former two are realised on all
%% nodes as it is a part of the Ra implementation log truncation mechanism.
%% The `garbage_collect' effects that is used to explicitly triggering a GC run
%% in the Ra servers' process.
%%
%% When the leader changes and when the cluster is restarted, there is a small chance
%% that effects are issued multiple times so designing effects with idempotency
%% in mind is a good idea.
%%
%%
%% <dl>
%% <dt><b>send_msg</b></dt>
%% <dd> send a message to a pid or registered process
%% NB: this is sent using `noconnect' and `nosuspend' options in order to avoid
%% blocking the ra process on connectivity failures. It can optionally be wrapped up as
%% a `ra_event' and/or as a gen cast message (``{'$cast', Msg}'')
%% </dd>
%% <dt><b>mod_call</b></dt>
%% <dd> Call an arbitrary Module:Function with the supplied arguments</dd>
%% <dt><b>monitor</b></dt>
%% <dd> monitor a process or erlang node</dd>
%% <dt><b>demonitor</b></dt>
%% <dd> demonitor a process or erlang node</dd>
%% <dt><b>release_cursor</b></dt>
%% <dd> indicate to Ra that none of the preceding entries contribute to the
%% current machine state</dd>
%% </dl>

-type effects() :: [effect()].
%% See: {@link effect}

-type reply() :: term().
%% an arbitrary term that can be returned to the caller, _if_ the caller
%% used {@link ra:process_command/2} or
%% {@link ra:process_command/3}

-type command(UserCommand) :: UserCommand | builtin_command().

-type command_meta_data() :: #{system_time := integer(),
                               index := ra_index(),
                               term := ra_term(),
                               machine_version => version(),
                               from => from(),
                               reply_mode => ra_server:command_reply_mode()}.
%% extensible command meta data map


-export_type([machine/0,
              version/0,
              effect/0,
              effects/0,
              reply/0,
              builtin_command/0,
              command/1,
              command_meta_data/0]).

-optional_callbacks([tick/2,
                     snapshot_installed/4,
                     state_enter/2,
                     init_aux/1,
                     handle_aux/5,
                     handle_aux/6,
                     overview/1,
                     live_indexes/1,
                     snapshot_module/0,
                     version/0,
                     which_module/1
                     ]).

-define(OPT_CALL(Call, Def),
    try Call of
        Res -> Res
    catch
        error:undef ->
            Def
    end).

-define(DEFAULT_VERSION, 0).

-callback init(Conf :: machine_init_args()) -> state().

-callback 'apply'(command_meta_data(), command(term()), State) ->
    {State, reply(), effects() | effect()} | {State, reply()} when State :: term().

%% Optional callbacks

-callback state_enter(ra_server:ra_state() | eol, state()) -> effects().

-callback tick(TimeMs :: milliseconds(), state()) -> effects().

-callback snapshot_installed(Meta, State, OldMeta, OldState) -> Effects
    when
      Meta :: ra_snapshot:meta(),
      State :: state(),
      OldMeta :: ra_snapshot:meta(),
      OldState :: state(),
      Effects :: effects().

-callback init_aux(Name :: atom()) -> AuxState :: term().

-callback handle_aux(ra_server:ra_state(),
                     {call, From :: from()} | cast,
                     Command :: term(),
                     AuxState,
                     IntState) ->
    {reply, Reply :: term(), AuxState, IntState} |
    {reply, Reply :: term(), AuxState, IntState, effects()} |
    {no_reply, AuxState, IntState} |
    {no_reply, AuxState, IntState, effects()}
      when AuxState :: term(),
           IntState :: ra_aux:internal_state().

-callback handle_aux(ra_server:ra_state(),
                     {call, From :: from()} | cast,
                     Command :: term(),
                     AuxState,
                     LogState,
                     MacState :: state()) ->
    {reply, Reply :: term(), AuxState, LogState} |
    {reply, Reply :: term(), AuxState, LogState,
     [{monitor, process, aux, pid()}]} |
    {no_reply, AuxState, LogState} |
    {no_reply, AuxState, LogState,
     [{monitor, process, aux, pid()}]}
      when AuxState :: term(),
           LogState :: ra_log:state().

-callback overview(state()) -> map().

-callback live_indexes(state()) -> [ra:index()].

-callback snapshot_module() -> module().

-callback version() -> version().

-callback which_module(version()) -> module().

%% @doc initialise a new machine
%% This is only called on startup only if there isn't yet a snapshot to recover
%% from. Once a snapshot has been taken this is never called again.
-spec init(machine(), atom(), version()) -> state().
init({machine, _, Args} = Machine, Name, Version) ->
    Mod = which_module(Machine, Version),
    Mod:init(Args#{name => Name,
                   machine_version => Version}).

-spec apply(module(), command_meta_data(), command(term()), State) ->
    {State, reply(), effects()} | {State, reply()}.
apply(Mod, Metadata, Cmd, State) ->
    Mod:apply(Metadata, Cmd, State).

-spec tick(module(), milliseconds(), state()) -> effects().
tick(Mod, TimeMs, State) ->
    ?OPT_CALL(Mod:tick(TimeMs, State), []).

-spec snapshot_installed(Module, Meta, State, OldMeta, OldState) ->
    effects() when
      Module :: module(),
      Meta :: ra_snapshot:meta(),
      State :: state(),
      OldMeta :: ra_snapshot:meta(),
      OldState :: state().
snapshot_installed(Mod, Meta, State, OldMeta, OldState)
  when is_atom(Mod) andalso
       is_map(Meta) andalso
       is_map(OldMeta) ->
    try
        Mod:snapshot_installed(Meta, State, OldMeta, OldState)
    catch
        error:undef ->
            try
                Mod:snapshot_installed(Meta, State)
            catch
                error:undef ->
                    []
            end
    end.

%% @doc called when the ra_server_proc enters a new state
-spec state_enter(module(), ra_server:ra_state() | eol, state()) ->
    effects().
state_enter(Mod, RaftState, State) ->
    ?OPT_CALL(Mod:state_enter(RaftState, State), []).

-spec overview(module(), state()) -> map().
overview(Mod, State) ->
    ?OPT_CALL(Mod:overview(State), State).

-spec live_indexes(module(), state()) -> [ra:index()].
live_indexes(Mod, State) ->
    ?OPT_CALL(Mod:live_indexes(State), []).

%% @doc used to discover the latest machine version supported by the current
%% code
-spec version(machine() | module()) -> version().
version(Mod) when is_atom(Mod) ->
    ?OPT_CALL(assert_version(Mod:version()), ?DEFAULT_VERSION);
version({machine, Mod, _}) ->
    version(Mod).

-spec is_versioned(machine()) -> boolean().
is_versioned({machine, Mod, _}) ->
    try
        _ = Mod:version(),
        true
    catch
        error:undef ->
            false
    end.

-spec which_module(machine(), version()) -> module().
which_module({machine, Mod, _}, Version) ->
    ?OPT_CALL(Mod:which_module(Version), Mod).

-spec init_aux(module(), atom()) -> term().
init_aux(Mod, Name) ->
    ?OPT_CALL(Mod:init_aux(Name), undefined).

-spec handle_aux(module(),
                 ra_server:ra_state(),
                 {call, From :: from()} | cast,
                 Command :: term(),
                 AuxState,
                 LogState,
                 MacState :: state()) ->
    undefined |
    {reply, Reply :: term(), AuxState, LogState} |
    {reply, Reply :: term(), AuxState, LogState,
     [{monitor, process, aux, pid()}]} |
    {no_reply, AuxState, LogState} |
    {no_reply, AuxState, LogState,
     [{monitor, process, aux, pid()}]}
      when AuxState :: term(),
           LogState :: ra_log:state().
handle_aux(Mod, RaftState, Type, Cmd, Aux, Log, MacState) ->
    Mod:handle_aux(RaftState, Type, Cmd, Aux, Log, MacState).


-spec handle_aux(module(),
                 ra_server:ra_state(),
                 {call, From :: from()} | cast,
                 Command :: term(),
                 AuxState,
                 State) ->
    {reply, Reply :: term(), AuxState, State} |
    {reply, Reply :: term(), AuxState, State,
     [{monitor, process, aux, pid()}]} |
    {no_reply, AuxState, State} |
    {no_reply, AuxState, State,
     [{monitor, process, aux, pid()}]}
      when AuxState :: term(),
           State :: ra_server:state().
handle_aux(Mod, RaftState, Type, Cmd, Aux, State) ->
    Mod:handle_aux(RaftState, Type, Cmd, Aux, State).

-spec which_aux_fun(module()) ->
    undefined | {atom(), arity()}.
which_aux_fun(Mod) when is_atom(Mod) ->
    case lists:sort([E || {handle_aux, _Arity} = E
                          <- erlang:apply(Mod,module_info, [exports])]) of
        [] ->
            undefined;
        [AuxFun | _] ->
            %% favour {handle_aux, 5} as this is the newer api
            AuxFun
    end.

-spec query(module(), fun((state()) -> Result), state()) ->
    Result when Result :: term().
query(Mod, Fun, State) when Mod =/= ra_machine_simple ->
    apply_fun(Fun, State);
query(ra_machine_simple, Fun, {simple, _, State}) ->
    apply_fun(Fun, State).

apply_fun(Fun, State) ->
    case Fun of
        F when is_function(F, 1) ->
            Fun(State);
        {M, F, A} ->
            erlang:apply(M, F, A ++ [State])
    end.

-spec module(machine()) -> module().
module({machine, Mod, _}) -> Mod.

-spec snapshot_module(machine()) -> module().
snapshot_module({machine, Mod, _}) ->
    ?OPT_CALL(Mod:snapshot_module(), ?DEFAULT_SNAPSHOT_MODULE).

%% internals

assert_version(I) when is_integer(I) andalso I >= 0 ->
    I.
