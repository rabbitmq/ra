-module(ra_machine).

-compile({no_auto_import, [apply/3]}).

-include("ra.hrl").


-export([init/2,
         apply/4,
         leader_effects/2,
         tick/3,
         overview/2,
         module/1
        ]).

-type state() :: term().
-type command() :: term().
-type apply_fun() :: fun((command(), state()) -> state()).

-type machine() :: {simple, apply_fun(), Initial :: state()} |
                   {module, module(), AddInitArgs :: #{atom() => term()}}.

-type milliseconds() :: non_neg_integer().

-type builtin_command() :: {down, pid(), term()} |
                           {nodeup | nodedown, node()}.

-type effect() ::
    {send_msg, pid() | atom() | {atom(), atom()}, term()} |
    {mod_call, module(), atom(), [term()]} |
    {monitor, process, pid()} |
    {monitor, node, node()} |
    {demonitor, pid()} |
    % indicates that none of the preceeding entries contribute to the
    % current machine state
    {release_cursor, ra_index(), state()} |
    % allows the machine to have a metrics table created as well as an
    % optional initial record
    {metrics_table, atom(), maybe(tuple())}.

-type effects() :: [effect()].

-type reply() :: term().
%% an arbitrary term that can be returned to the caller, _if_ the caller
%% used {@link ra:send_and_await_consensus/2} or
%% {@link ra:send_and_await_consensus/3}

-type machine_init_args() :: #{name := atom(),
                               atom() => term()}.
%% the configuration passed to the init callback


-export_type([machine/0,
              effect/0,
              effects/0,
              reply/0,
              builtin_command/0]).


-callback init(Conf :: machine_init_args()) -> {state(), effects()}.

%% Applies each entry to the state machine.
%% returns the new updated state and a list of effects
-callback 'apply'(Index :: ra_index(), command(), state()) ->
    {state(), effects()} | {state(), effects(), reply()}.
%% Applies each entry to the state machine.

%% called when a node becomes leader, use this to return any effects that should
%% be applied only to a leader, such as monitors
-callback leader_effects(state()) -> effects().

%% Called periodically
%% suitable for returning granular metrics or other periodic actions
-callback tick(TimeMs :: milliseconds(),
               state()) -> effects().

%% a map of overview information - needs to be efficient
-callback overview(state()) -> map().

-spec init(machine(), atom()) -> {state(), effects()}.
init({module, Mod, Args}, Name) ->
    Mod:init(Args#{name => Name});
init({simple, _Fun, InitialState}, _Name) ->
    {InitialState, []}.

-spec apply(machine(), ra_index(), command(), state()) ->
    {state(), effects()} | {state(), effects(), reply()}.
apply({module, Mod, _}, Idx, Cmd, State) ->
    Mod:apply(Idx, Cmd, State);
apply({simple, Fun, _InitialState}, _Idx, Cmd, State) ->
    {Fun(Cmd, State), []}.

-spec leader_effects(machine(), state()) -> effects().
leader_effects({module, Mod, _}, State) ->
    Mod:leader_effects(State);
leader_effects({simple, _, _}, _State) ->
    [].

-spec tick(machine(), milliseconds(), state()) -> effects().
tick({module, Mod, _}, TimeMs, State) ->
    Mod:tick(TimeMs, State);
tick({simple, _, _}, _TimeMs, _State) ->
    [].

-spec overview(machine(), state()) -> map().
overview({module, Mod, _}, State) ->
    Mod:overview(State);
overview({simple, _, _}, _State) ->
    #{type => simple}.

-spec module(machine()) -> undefined | module().
module({module, Mod, _}) -> Mod;
module(_) -> undefined.

