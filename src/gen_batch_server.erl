-module(gen_batch_server).

-export([start_link/4,
         init_it/6,
         cast/2,
         call/2,
         call/3,
         system_continue/3,
         system_terminate/4,
         system_get_state/1,
         write_debug/3,
         format_status/2
        ]).

-define(MIN_MAX_BATCH_SIZE, 32).
-define(MAX_MAX_BATCH_SIZE, 8192).

-type server_ref() :: pid() |
                      (LocalName :: atom()) |
                      {Name :: atom(), Node :: atom()}.

-type from() :: {Pid :: server_ref(), Tag :: reference()}.

-type op() :: {cast, server_ref(), UserOp :: term()} |
              {call, from(), UserOp :: term()}.

-record(state, {batch = [] :: [op()],
                batch_count = 0 :: non_neg_integer(),
                batch_size = ?MIN_MAX_BATCH_SIZE :: non_neg_integer(),
                min_batch_size = ?MIN_MAX_BATCH_SIZE :: non_neg_integer(),
                max_batch_size = ?MAX_MAX_BATCH_SIZE :: non_neg_integer(),
                parent :: pid(),
                name :: atom(),
                module :: module(),
                state :: term(),
                debug :: list()
               }).

% -type state() :: #state{}.

-export_type([from/0, op/0,
              action/0, server_ref/0]).

%%% Behaviour

-type action() ::
    {reply, from(), Msg :: term()} |
    {notify, pid(), Msg :: term()}.
%% action that can be returned from handle_batch/2

-callback init(Args :: term()) ->
    {ok, State} |
    {stop, Reason :: term()}
      when State :: term().

-callback handle_batch([op()], State) ->
    {ok, State} |
    {ok, [action()], State} |
    {stop, Reason :: term()}
      when State :: term().

-callback terminate(Reason :: term(), State :: term()) -> term().

-callback format_status(State :: term()) -> term().

-optional_callbacks([format_status/1]).

%% TODO: code_change

%%%
%%% API
%%%

-spec start_link({local, atom()}, module(), Args :: term(),
                 Options :: list()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_link(Name, Mod, Args, Opts) ->
    gen:start(?MODULE, link, Name, Mod, Args, Opts).

%% pretty much copied wholesale from gen_server
init_it(Starter, self, Name, Mod, Args, Options) ->
    init_it(Starter, self(), Name, Mod, Args, Options);
init_it(Starter, Parent, Name0, Mod, Args, Options) ->
    Name = gen:name(Name0),
    Debug = gen:debug_options(Name, Options),
    case catch Mod:init(Args) of
        {ok, State0} ->
            proc_lib:init_ack(Starter, {ok, self()}),
            State = #state{module = Mod, state = State0,
                           parent = Parent,
                           name = Name, debug = Debug},
            loop_wait(State, Parent);
        {stop, Reason} ->
            %% For consistency, we must make sure that the
            %% registered name (if any) is unregistered before
            %% the parent process is notified about the failure.
            %% (Otherwise, the parent process could get
            %% an 'already_started' error if it immediately
            %% tried starting the process again.)
            gen:unregister_name(Name0),
            proc_lib:init_ack(Starter, {error, Reason}),
            exit(Reason);
        % ignore ->
        %     gen:unregister_name(Name0),
        %     proc_lib:init_ack(Starter, ignore),
        %     exit(normal);
        {'EXIT', Reason} ->
            gen:unregister_name(Name0),
            proc_lib:init_ack(Starter, {error, Reason}),
            exit(Reason);
        Else ->
            Error = {bad_return_value, Else},
            proc_lib:init_ack(Starter, {error, Error}),
            exit(Error)
    end.

-spec cast(pid() | atom(), term()) -> ok.
cast(Ref, Msg) ->
    try Ref ! {'$gen_cast', self(), Msg} of
        _ -> ok
    catch
        _:_ -> ok
    end.

-spec call(pid() | atom(), term()) -> term().
call(Name, Request) ->
    call(Name, Request, 5000).

-spec call(pid() | atom(), term(), non_neg_integer()) -> term().
call(Name, Request, Timeout) ->
    case catch gen:call(Name, '$gen_call', Request, Timeout) of
        {ok,Res} ->
            Res;
        {'EXIT',Reason} ->
            exit({Reason, {?MODULE, call, [Name, Request, Timeout]}})
    end.


%% Internal

loop_wait(State0, Parent) ->
    %% batches can accumulate a lot of garbage, collect it here
    garbage_collect(),
    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent,
                                  ?MODULE, State0#state.debug, State0);
        {'EXIT', Parent, Reason} ->
            terminate(Reason, State0),
            exit(Reason);
        Msg ->
            enter_loop_batched(Msg, Parent, State0)
    end.

append_msg({'$gen_cast', Pid, Msg},
           #state{batch = Batch,
                  batch_count = BatchCount} = State0) ->
    State0#state{batch = [{cast, Pid, Msg} | Batch],
                 batch_count = BatchCount + 1};
append_msg({'$gen_call', From, Msg},
           #state{batch = Batch,
                  batch_count = BatchCount} = State0) ->
    State0#state{batch = [{call, From, Msg} | Batch],
                 batch_count = BatchCount + 1}.

enter_loop_batched(Msg, Parent, #state{debug = []} = State0) ->
    loop_batched(append_msg(Msg, State0), Parent);
enter_loop_batched(Msg, Parent, State0) ->
    State = handle_debug_in(State0, Msg),
    %% append to batch
    loop_batched(append_msg(Msg, State), Parent).

loop_batched(#state{batch_size = Written,
                    max_batch_size = Max,
                    batch_count = Written} = State0,
             Parent) ->
    % complete batch after seeing batch_size writes
    State = complete_batch(State0),
    % grow max batch size
    NewBatchSize = min(Max, Written * 2),
    loop_wait(State#state{batch_size = NewBatchSize}, Parent);
loop_batched(#state{debug = Debug} = State0, Parent) ->
    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent,
                                  ?MODULE, Debug, State0);
        {'EXIT', Parent, Reason} ->
            terminate(Reason, State0),
            exit(Reason);
        Msg ->
            enter_loop_batched(Msg, Parent, State0)
    after 0 ->
              State = complete_batch(State0),
              NewBatchSize = max(State#state.min_batch_size,
                                 State#state.batch_size / 2),
              loop_wait(State#state{batch_size = NewBatchSize}, Parent)
    end.

terminate(Reason, #state{module = Mod, state = Inner}) ->
    catch Mod:terminate(Reason, Inner),
    ok.


complete_batch(#state{batch = []} = State) ->
    State;
complete_batch(#state{batch = Batch,
                      module = Mod,
                      state = Inner0,
                      debug = Debug0} = State0) ->
    case catch Mod:handle_batch(lists:reverse(Batch), Inner0) of
        {ok, Actions, Inner} ->
            State = State0#state{batch = [], state = Inner, batch_count = 0},
            Debug = lists:foldl(fun ({reply, {Pid, Tag}, Msg}, Dbg) ->
                                        Pid ! {Tag, Msg},
                                        handle_debug_out(Pid, Msg, Dbg);
                                    ({notify, Pid, Msg}, Dbg) ->
                                        Pid ! Msg,
                                        handle_debug_out(Pid, Msg, Dbg)
                                end, Debug0, Actions),
            State#state{debug = Debug};
        {stop, Reason} ->
            terminate(Reason, State0),
            exit(Reason);
        {'EXIT', Reason} ->
            terminate(Reason, State0),
            exit(Reason)
    end.

handle_debug_in(#state{debug = Dbg0} = State, Msg) ->
    Dbg = sys:handle_debug(Dbg0, fun write_debug/3, ?MODULE, {in, Msg}),
    State#state{debug = Dbg}.

handle_debug_out(_, _, []) ->
    [];
handle_debug_out(Pid, Msg, Dbg) ->
    Evt = {out, {self(), Msg}, Pid},
    sys:handle_debug(Dbg, fun write_debug/3, ?MODULE, Evt).

%% Here are the sys call back functions

system_continue(Parent, Debug, State) ->
    % TODO check if we've written to the current batch or not
    loop_batched(State#state{debug = Debug}, Parent).

system_terminate(Reason, _Parent, _Debug, State) ->
    terminate(Reason, State),
    exit(Reason).

system_get_state(State) ->
    {ok, State}.

format_status(_Reason, [_PDict, SysState, Parent, Debug,
                        #state{name = Name,
                               module = Mod,
                               state = State }]) ->
    Header = gen:format_status_header("Status for batching server", Name),
    Log = sys:get_debug(log, Debug, []),
    [{header, Header},
     {data,
      [
       {"Status", SysState},
       {"Parent", Parent},
       {"Logged Events",Log}]} |
     case catch Mod:format_status(State) of
         L when is_list(L) -> L;
         T -> [T]
     end].

write_debug(Dev, Event, Name) ->
    io:format(Dev, "~p event = ~p~n", [Name, Event]).
