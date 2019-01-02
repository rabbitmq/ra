-module(noop).

-behaviour(ra_machine).

-compile(inline_list_funcs).
-compile(inline).
-compile({no_auto_import, [apply/3]}).

-include("ra.hrl").

-include_lib("eunit/include/eunit.hrl").
-export([
         init/1,
         apply/3,

         start/1,
         spawn_client/1,
         print_metrics/1,

         prepare/0,
         profile/0,
         stop_profile/0
        ]).


init(#{}) ->
    undefined.

% msg_ids are scoped per customer
% ra_indexes holds all raft indexes for enqueues currently on queue
apply(#{index := I}, {noop, _}, State) ->
    case I rem 100000 of
        0 ->
            {State, ok, {release_cursor, I, State}};
        _ ->
            {State, ok}
    end.


start(Nodes) ->
    application:ensure_all_started(ra),
    Servers = [begin
                   rpc:call(N, ?MODULE, prepare, []),
                   {noop, N}
               end || N <- Nodes],
    {ra:start_cluster(noop, {module, ?MODULE, #{}}, Servers),
     Servers}.

prepare() ->
    application:ensure_all_started(ra),
    % error_logger:logfile(filename:join(ra_env:data_dir(), "log.log")),
    ok.

send_n(_, 0) -> ok;
send_n(Leader, N) ->
    ra:pipeline_command(Leader, {noop, <<>>}, make_ref(), normal),
    send_n(Leader, N-1).


client_loop(Leader) ->
    receive
        {ra_event, Leader, {applied, Applied}} ->
            N = length(Applied),
            send_n(Leader, N),
            client_loop(Leader);
        {ra_event, Leader, Evt} ->
            io:format("unexpected ra_event ~w", [Evt]),
            client_loop(Leader)
    end.

spawn_client(Servers) ->
    spawn_link(
      fun () ->
              %% first send one 1000 noop commands
              %% then top up as they are applied
              {ok, _, Leader} = ra:members(hd(Servers)),
              send_n(Leader, 1000),
              client_loop(Leader)
      end).

print_metrics(undefined) ->
    print_metrics(hd(ets:lookup(ra_metrics, noop)));
print_metrics({noop, A0, B0}) ->
    timer:sleep(1000),
    [{noop, A, B} = X] = ets:lookup(ra_metrics, noop),
    io:format("metrics ~b ~b per second~n",
              [A-A0, B-B0]),
    print_metrics(X).


profile() ->
    GzFile = atom_to_list(node()) ++ ".gz",
    lg:trace([noop, ra_server, ra_server_proc, ra_snapshot, ra_machine,
              ra_log, ra_flru, ra_machine, ra_log_meta, ra_log_segment],
             lg_file_tracer,
             GzFile, #{running => false, mode => profile}),
    ok.

stop_profile() ->
    lg:stop(),
    Base = atom_to_list(node()),
    GzFile = Base ++ ".gz.*",
    lg_callgrind:profile_many(GzFile, Base ++ ".out",#{}),
    ok.

