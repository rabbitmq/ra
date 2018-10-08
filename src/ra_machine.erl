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
%%<br></br>
%% <code>-callback apply(Meta :: command_meta_data(),
%%                       {@link command()}, effects(), State) ->
%%    {State, {@link effects()}, {@link reply()}}</code>
%%
%% Applies each entry to the state machine. Effects should be prepended to the
%% incoming list of effects. Ra will reverse the list of effects before
%% processing.
%%
%%<br></br>
%% <code>-callback state_enter(ra_server:ra_state() | eol, state()) -> effects().
%% </code>
%%
%% Optional. Called when the ra server enters a new state. Called for all states
%% in the ra_server_proc gen_statem implementation not just for the standard
%% Raft states (follower, candidate, leader). If implemented it is sensible
%% to include a catch all clause as new states may be implemented in the future.
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
         apply/5,
         tick/3,
         state_enter/3,
         overview/2,
         query/3,
         module/1,
         init_aux/2,
         handle_aux/7
        ]).

-type state() :: term().
%% The state for a given machine implementation.

-type user_command() :: term().
%% the command type for a given machine implementation

-type machine_init_args() :: #{name := atom(), atom() => term()}.
%% the configuration passed to the init callback

-type machine() :: {machine, module(), AddInitArgs :: #{term() => term()}}.
%% Machine configuration.
%% the `module()' should implement the {@link ra_machine} behaviour.

-type milliseconds() :: non_neg_integer().

-type builtin_command() :: {down, pid(), term()} |
                           {nodeup | nodedown, node()}.
%% These commands may be passed to the {@link apply/2} function in reaction
%% to monitor effects

-type send_msg_opt() :: [ra_event | cast].
%% ra_event: the message will be wrapped up and sent as a ra event
%% e.g: `{ra_event, ra_server_id(), Msg}'
%%
%% cast: the message will be wrapped as a gen cast: ``{'$cast', Msg}''

-type send_msg_opts() :: send_msg_opt() | [send_msg_opt()].
-type locator() :: pid() | atom() | {atom(), node()}.

-type effect() ::
    {send_msg, To :: locator(), Msg :: term()} |
    {send_msg, To :: locator(), Msg :: term(), Options :: send_msg_opts()} |
    {mod_call, module(), Function :: atom(), [term()]} |
    {monitor, process, pid()} |
    {monitor, node, node()} |
    {demonitor, process, pid()} |
    {demonitor, node, node()} |
    {release_cursor, ra_index(), state()} |
    {aux, term()} |
    garbage_collection.
%% Effects are data structure that can be returned by {@link apply/4} to ask
%% ra to realise a side-effect in the real works, such as sending
%% a message to a process.
%%
%% Although both leaders and followers will process the same commands effects
%% are typically only applied on the leader. The only exceptions to this are
%% the `relaase_cursor' effect that is realised on all effects as it is part
%% of the ra implementation log truncation mechanism and the `garbage_collect'
%% effects that is used to explicitly triggering a garbage collection in the
%% ra servers's process.
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
%% blocking the ra process during failures. It can optionally be wrapped up as
%% a `ra_event' and/or as a gen cast message (``{'$cast', Msg}'')
%% </dd>
%% <dt><b>mod_call</b></dt>
%% <dd> Call an arbitrary Module:Function with the supplied arguments </dd>
%% <dt><b>monitor</b></dt>
%% <dd> monitor a process or erlang node </dd>
%% <dt><b>demonitor</b></dt>
%% <dd> demonitor a process or erlang node </dd>
%% <dt><b>release_cursor</b></dt>
%% <dd> indicate to Ra that none of the preceeding entries contribute to the
%% current machine state </dd>
%% </dl>

-type effects() :: [effect()].
%% See: {@link effect}

-type reply() :: term().
%% an arbitrary term that can be returned to the caller, _if_ the caller
%% used {@link ra:process_command/2} or
%% {@link ra:process_command/3}

-type command() :: user_command() | builtin_command().

-type command_meta_data() :: ra_server:command_meta() | #{index := ra_index(),
                                                         term := ra_term()}.
%% extensible command meta data map


-export_type([machine/0,
              effect/0,
              effects/0,
              reply/0,
              builtin_command/0,
              command_meta_data/0]).

-optional_callbacks([tick/2,
                     state_enter/2,
                     init_aux/1,
                     handle_aux/6,
                     overview/1
                     ]).

-define(OPT_CALL(Call, Def),
    %% TODO: this will swallow genuine error:undef errors
    try Call of
        Res -> Res
    catch
        error:undef ->
            Def
    end).

-callback init(Conf :: machine_init_args()) -> state().

-callback 'apply'(command_meta_data(), command(), effects(), State) ->
    {State, effects(), reply()}.

-callback state_enter(ra_server:ra_state() | eol, state()) -> effects().

-callback tick(TimeMs :: milliseconds(), state()) -> effects().

-callback init_aux(Name :: atom()) -> term().

-callback handle_aux(ra_server:ra_state(),
                     {call, From :: from()} | cast,
                     Command :: term(),
                     AuxState,
                     LogState,
                     MacState :: state()) ->
    {reply, Reply :: term(), AuxState, LogState} |
    {no_reply, AuxState, LogState}
      when AuxState :: term(),
           LogState :: ra_log:ra_log().

-callback overview(state()) -> map().

%% @doc initialise a new machine
-spec init(machine(), atom()) -> state().
init({machine, Mod, Args}, Name) ->
    Mod:init(Args#{name => Name}).

-spec apply(machine(), command_meta_data(), command(), effects(), State) ->
    {State, effects(), reply()}.
apply({machine, Mod, _}, Idx, Cmd, Effects, State) ->
    Mod:apply(Idx, Cmd, Effects, State).

-spec tick(machine(), milliseconds(), state()) -> effects().
tick({machine, Mod, _}, TimeMs, State) ->
    ?OPT_CALL(Mod:tick(TimeMs, State), []).

%% @doc called when the ra_server_proc enters a new state
-spec state_enter(machine(), ra_server:ra_state() | eol, state()) ->
    effects().
state_enter({machine, Mod, _}, RaftState, State) ->
    ?OPT_CALL(Mod:state_enter(RaftState, State), []).

-spec overview(machine(), state()) -> map().
overview({machine, Mod, _}, State) ->
    ?OPT_CALL(Mod:overview(State), State).

-spec init_aux(machine(), atom()) -> term().
init_aux({machine, Mod, _}, Name) ->
    ?OPT_CALL(Mod:init_aux(Name), undefined).

-spec handle_aux(machine(), ra_server:ra_state(),
                 {call, From :: from()} | cast,
                 Command :: term(), AuxState,
                 LogState, MacState :: state()) ->
    {reply, Reply :: term(), AuxState, LogState} |
    {no_reply, AuxState, LogState} |
    undefined
      when AuxState :: term(),
           LogState :: ra_log:ra_log().
handle_aux({machine, Mod, _}, RaftState, Type, Cmd, Aux, Log, MacState) ->
    ?OPT_CALL(Mod:handle_aux(RaftState, Type, Cmd, Aux, Log, MacState),
              undefined).

-spec query(module(), fun((state()) -> Result), state()) ->
    Result when Result :: term().
query(Mod, Fun, State) when Mod =/= ra_machine_simple ->
    Fun(State);
query(ra_machine_simple, Fun, {simple, _, State}) ->
    Fun(State).

-spec module(machine()) -> module().
module({machine, Mod, _}) -> Mod.

