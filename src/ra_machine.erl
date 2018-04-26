%% @doc The `ra_machine' behaviour.
%%
%% Used to implement the logic for the state machine running inside Ra.
%%
%% == Callbacks ==
%%
%% <code>-callback init(Conf :: {@link machine_init_args()}) -> {state(), effects()}'</code>
%%
%% Initialize a new machine state.
%%
%%<br></br>
%% <code>-callback apply(Index :: ra_index(), {@link command()}, State) ->
%%    {State, effects()} | {State, {@link effects()}, {@link reply()}}</code>
%%
%% Applies each entry to the state machine.
%%
%%<br></br>
%% <code>-callback leader_effects(state()) -> effects().</code>
%%
%% Optional. Called when a node becomes leader, use this to return any effects
%% need to be issued at this point.
%%
%%<br></br>
%% <code>-callback eol_effects(state()) -> effects().</code>
%%
%% Optional. Called when a ra cluster is deleted.
%%
%%<br></br>
%% <code>-callback tick(TimeMs :: milliseconds(), state()) -> effects().</code>
%%
%% Optional. Called periodically.
%% Suitable for issuing periodic side effects such as updating metrics systems.
%%
%%<br></br>
%% <code>-callback overview(state()) -> map(). </code>
%%
%% Optional. A map of overview information. Needs to be efficient.

-module(ra_machine).

-compile({no_auto_import, [apply/3]}).

-include("ra.hrl").


-export([init/2,
         apply/4,
         leader_effects/2,
         eol_effects/2,
         tick/3,
         overview/2,
         module/1
        ]).

-type state() :: term().
%% The state for a given machine implementation.

-type user_command() :: term().
%% the command type for a given machine implementation

-type apply_fun(State) :: fun((user_command(), State) -> State).

-type machine_init_args() :: #{name := atom(), atom() => term()}.
%% the configuration passed to the init callback

-type machine() :: {simple, apply_fun(State), State} |
                   {module, module(), AddInitArgs :: #{term() => term()}}.
%% Machine configuration.
%% the `module()' should implement the {@link ra_machine} behaviour.

-type milliseconds() :: non_neg_integer().

-type builtin_command() :: {down, pid(), term()} |
                           {nodeup | nodedown, node()}.
%% These commands may be passed to the {@link apply/2} function in reaction
%% to monitor effects

-type effect() ::
    {send_msg, pid() | atom() | {atom(), atom()}, term()} |
    {mod_call, module(), Function :: atom(), [term()]} |
    {monitor, process, pid()} |
    {monitor, node, node()} |
    {demonitor, pid()} |
    {release_cursor, ra_index(), state()} |
    {metrics_table, atom(), maybe(tuple())}.
%% Effects are data structure that can be returned by {@link apply/2} to ask
%% ra to realise a side-effect in the real works, such as sending a message to a process.
%%
%% Although both leaders and followers will process the same commands effects
%% are typically only applied on the leader. The only exception to this is
%% the `relaase_cursor' effect that is realised on all effects as it is part
%% of the ra implementation log truncation mechanism.
%%
%% When leaders change and when clusters are restarted there is a small chance
%% that effects are issued multiple times so designing effects to be idempotent
%% is a good idea.
%%
%%
%% <dl>
%% <dt><b>send_msg</b></dt>
%% <dd> send a message to a pid or registered process
%% NB: this is sent using `noconnect' and `nosuspend' in order to avoid
%% blocking the ra process during failures.
%% </dd>
%% <dt><b>mod_call</b></dt>
%% <dd> Call an arbitrary Module:Function with the supplied arguments </dd>
%% <dt><b>monitor</b></dt>
%% <dd> monitor a process or erlang node </dd>
%% <dt><b>demonitor</b></dt>
%% <dd> demonitor a process </dd>
%% <dt><b>release_cursor</b></dt>
%% <dd> indicate to Ra that none of the preceeding entries contribute to the
%% current machine state </dd>
%% </dl>

-type effects() :: [effect()].
%% See: {@link effect}

-type reply() :: term().
%% an arbitrary term that can be returned to the caller, _if_ the caller
%% used {@link ra:send_and_await_consensus/2} or
%% {@link ra:send_and_await_consensus/3}

-type command() :: user_command() | builtin_command().

-export_type([machine/0,
              effect/0,
              effects/0,
              reply/0,
              builtin_command/0]).

-optional_callbacks([leader_effects/1,
                     tick/2,
                     overview/1,
                     eol_effects/1
                     ]).

-define(OPT_CALL(Call, Def),
    try Call of
        Res -> Res
    catch
        error:undef ->
            Def
    end).

-callback init(Conf :: machine_init_args()) -> {state(), effects()}.

-callback 'apply'(Index :: ra_index(), command(), State) ->
    {State, effects()} | {State, effects(), reply()}.

-callback leader_effects(state()) -> effects().

-callback eol_effects(state()) -> effects().

-callback tick(TimeMs :: milliseconds(), state()) -> effects().

-callback overview(state()) -> map().

%% @doc initialise a new machine
-spec init(machine(), atom()) -> {state(), effects()}.
init({module, Mod, Args}, Name) ->
    Mod:init(Args#{name => Name});
init({simple, _Fun, InitialState}, _Name) ->
    {InitialState, []}.

-spec apply(machine(), ra_index(), command(), State) ->
    {State, effects()} | {State, effects(), reply()}.
apply({module, Mod, _}, Idx, Cmd, State) ->
    Mod:apply(Idx, Cmd, State);
apply({simple, Fun, _InitialState}, _Idx, Cmd, State) ->
    {Fun(Cmd, State), []}.

-spec leader_effects(machine(), state()) -> effects().
leader_effects({module, Mod, _}, State) ->
    ?OPT_CALL(Mod:leader_effects(State), []);
leader_effects({simple, _, _}, _State) ->
    [].

-spec eol_effects(machine(), state()) -> effects().
eol_effects({module, Mod, _}, State) ->
    ?OPT_CALL(Mod:eol_effects(State), []);
eol_effects({simple, _, _}, _State) ->
    [].

-spec tick(machine(), milliseconds(), state()) -> effects().
tick({module, Mod, _}, TimeMs, State) ->
    ?OPT_CALL(Mod:tick(TimeMs, State), []);
tick({simple, _, _}, _TimeMs, _State) ->
    [].

-spec overview(machine(), state()) -> map().
overview({module, Mod, Ms}, State) ->
    ?OPT_CALL(Mod:overview(State), Ms);
overview({simple, _, _}, _State) ->
    #{type => simple}.

-spec module(machine()) -> undefined | module().
module({module, Mod, _}) -> Mod;
module(_) -> undefined.

