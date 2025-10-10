-module(ra_kv_harness).

-export([
         run/1,
         run/2,
         read_all_keys/0,
         teardown_cluster/1,
         timestamp/0,
         log/2
        ]).

-include_lib("eunit/include/eunit.hrl").

-define(SYS, default).
-define(CLUSTER_NAME, kv).
-define(TIMEOUT, 30000).
-define(MAX_KEY, 10000). % Limited key space for more conflicts
-define(MIN_VALUE_SIZE, 1).
-define(MAX_VALUE_SIZE, 10_000_000). % 10MB
-define(MAX_NODES, 7). % Maximum number of nodes in the cluster

-type state() :: #{options => map(),
                   members := #{ra:server_id() => peer:server_ref()},
                   reference_map := #{binary() => term()},
                   operations_count := non_neg_integer(),
                   successful_ops := non_neg_integer(),
                   failed_ops := non_neg_integer(),
                   next_node_id := pos_integer(),
                   remaining_ops := non_neg_integer(),
                   consistency_failed := boolean(),
                   partition_state => #{partitioned_node => ra:server_id(),
                                        heal_time => non_neg_integer(),
                                        other_nodes => [node()]}}.

-spec timestamp() -> string().
timestamp() ->
    {MegaSecs, Secs, MicroSecs} = os:timestamp(),
    {{Year, Month, Day}, {Hour, Min, Sec}} = calendar:now_to_local_time({MegaSecs, Secs, MicroSecs}),
    Millisecs = MicroSecs div 1000,
    io_lib:format("[~4..0w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w.~3..0w]",
                  [Year, Month, Day, Hour, Min, Sec, Millisecs]).

milliseconds() ->
    erlang:system_time(millisecond).

-spec log(string(), list()) -> ok.
log(Format, Args) ->
    Message = io_lib:format(Format, Args),
    io:format("~s", [Message]),
    file:write_file("ra_kv_harness.log", Message, [append]).

-spec new_state() -> state().
new_state() ->
    #{members => #{},
      reference_map => #{},
      operations_count => 0,
      successful_ops => 0,
      failed_ops => 0,
      next_node_id => 1,
      remaining_ops => 0,
      consistency_failed => false}.

-type operation() :: {put, Key :: binary(), Value :: term()} |
                     {get, Key :: binary()} |
                     {snapshot} |
                     {major_compaction} |
                     {update_almost_all_keys} |
                     {kill_wal} |
                     {kill_member} |
                     {add_member} |
                     {remove_member} |
                     {network_partition}.

-spec run(NumOperations :: pos_integer()) ->
    {ok, #{successful := non_neg_integer(),
           failed := non_neg_integer(),
           consistency_checks := non_neg_integer()}} |
    {error, term()}.
run(NumOperations) ->
    run(NumOperations, #{}).

read_all_keys() ->
        [_ = ra_kv:get({?CLUSTER_NAME,
                        node()},
                       <<"key_",(integer_to_binary(N))/binary>>, 1000)
         || N <- lists:seq(1, ?MAX_KEY)],
        ok.

read_all_keys_loop(Members0) when is_list(Members0) ->
    receive
        stop ->
            log("~s Read all keys loop stopped~n", [timestamp()]),
            ok
    after 0 ->
              %% resolve current members
              {ok, Members, _} = ra:members(Members0),
              Member = hd(Members),
              NodeName = element(2, Member),
              log("~s Begin reading all keys on member ~0p~n", [timestamp(), Member]),
              T1 = erlang:monotonic_time(),
              ok = erpc:call(NodeName, ra_kv_harness, read_all_keys, []),
              T2 = erlang:monotonic_time(),
              Diff = erlang:convert_time_unit(T2 - T1, native, millisecond),
              log("~s Read all keys on member ~0p in ~bms~n", [timestamp(), Member, Diff]),
              read_all_keys_loop(Members)
    end.

-spec run(NumOperations :: pos_integer(),
          Options :: map()) ->
    {ok, #{successful := non_neg_integer(),
           failed := non_neg_integer(),
           consistency_checks := non_neg_integer()}} |
    {error, term()}.
run(NumOperations, Options) when NumOperations > 0 ->
    % Start with a random number of nodes between 1 and 7
    NumNodes = rand:uniform(7),
    logger:set_primary_config(level, warning),
    application:set_env(sasl, sasl_error_logger, false),
    application:stop(sasl),
    log("~s Starting cluster with ~p nodes~n", [timestamp(), NumNodes]),
    case setup_cluster(NumNodes, Options) of
        {ok, Members, PeerNodes} ->
            MembersMap = maps:from_list(lists:zip(Members, PeerNodes)),
            InitialState = (new_state())#{members => MembersMap,
                                          next_node_id => NumNodes + 1,
                                          remaining_ops => NumOperations,
                                          options => Options},
            %% keep reading all keys while the other operations are running
            ReaderPid = spawn(fun() -> read_all_keys_loop(maps:keys(MembersMap)) end),
            try
                State = execute_operation(InitialState, {put, <<"never_updated">>, <<"never_updated">>}),
                FinalState = run_operations(State, ?CLUSTER_NAME),
                ReaderPid ! stop,
                case maps:get(consistency_failed, FinalState, false) of
                    true ->
                        log("~s EMERGENCY STOP: Leaving cluster running for investigation~n", [timestamp()]),
                        {error, {consistency_failure, FinalState}};
                    false ->
                        ConsistencyChecks = validate_final_consistency(FinalState),
                        teardown_cluster(FinalState),
                        {ok, #{successful => maps:get(successful_ops, FinalState),
                               failed => maps:get(failed_ops, FinalState),
                               consistency_checks => ConsistencyChecks}}
                end
            catch
                Class:Reason:Stack ->
                    ReaderPid ! stop,
                    teardown_cluster(InitialState),
                    {error, {Class, Reason, Stack}}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

setup_cluster(NumNodes, Opts) when NumNodes > 0 ->
    % Start peer nodes
    case start_peer_nodes(NumNodes, Opts) of
        {ok, PeerNodes, NodeNames} ->
            Members = [{?CLUSTER_NAME, NodeName} || NodeName <- NodeNames],
            case ra_kv:start_cluster(?SYS, ?CLUSTER_NAME, #{members => Members}) of
                {ok, StartedMembers, _} ->
                    log("~s Started cluster with ~p members~n", [timestamp(), length(StartedMembers)]),
                    {ok, StartedMembers, PeerNodes};
                {error, Reason} ->
                    [peer:stop(PeerRef) || PeerRef <- PeerNodes],
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

start_peer_nodes(NumNodes, Opts) ->
    start_peer_nodes(NumNodes, [], [], Opts).

start_peer_nodes(0, PeerRefs, NodeNames, _Opts) ->
    {ok, lists:reverse(PeerRefs), lists:reverse(NodeNames)};
start_peer_nodes(N, PeerRefs, NodeNames, Opts) when N > 0 ->
    case start_single_peer_node(N, Opts) of
        {ok, PeerRef, NodeName} ->
            start_peer_nodes(N - 1, [PeerRef | PeerRefs], [NodeName | NodeNames], Opts);
        {error, Reason} ->
            % Clean up any already started peers
            [peer:stop(PeerRef) || PeerRef <- PeerRefs],
            {error, Reason}
    end.

start_single_peer_node(NodeId, Opts) ->
    NodeName = list_to_atom("ra_test_" ++ integer_to_list(NodeId) ++ "@" ++
                            inet_db:gethostname()),

    log("~s Starting node ~p nodes~n", [timestamp(), NodeName]),
    % Get all code paths from current node
    CodePaths = code:get_path(),
    PaArgs = lists:flatmap(fun(Path) -> ["-pa", Path] end, CodePaths),
    BufferSize = ["+zdbbl", "102400"],

    % Check if inet_tcp_proxy_dist is available
    % If yes, add -proto_dist argument at the front with its path, like erlang_node_helpers does
    ProtoDistArgs = case code:where_is_file("inet_tcp_proxy_dist.beam") of
        non_existing ->
            % inet_tcp_proxy not available, don't use it
            log("~s WARNING: inet_tcp_proxy_dist not found, starting peer without it~n", [timestamp()]),
            [];
        BeamPath ->
            DistModPath = filename:dirname(BeamPath),
            ["-pa", DistModPath, "-proto_dist", "inet_tcp_proxy",
             "-kernel", "prevent_overlapping_partitions", "false"]
    end,

    case peer:start_link(#{name => NodeName,
                           args => ProtoDistArgs ++ PaArgs ++ BufferSize,
                           connection => standard_io}) of
        {ok, PeerRef, NodeName} ->
            BaseDir = maps:get(dir, Opts, ""),
            erpc:call(NodeName, logger, set_primary_config, [level, warning]),
            erpc:call(NodeName, application, set_env, [sasl, sasl_error_logger, false]),
            erpc:call(NodeName, application, stop, [sasl]),
            Dir = filename:join(BaseDir, NodeName),
            {ok, _} = erpc:call(NodeName, ra, start_in, [Dir]),
            % Set logger level to reduce verbosity on peer node
            % Start ra application on the new peer node
            {ok, PeerRef, NodeName};
        {error, Reason} ->
            {error, Reason}
    end.

start_new_peer_node(NodeId, Opts) ->
    start_single_peer_node(NodeId, Opts).

-spec teardown_cluster(state()) -> ok.
teardown_cluster(#{members := Members}) ->
    % Stop Ra servers on each node and stop peer nodes
    maps:foreach(fun(Member, PeerRef) ->
                         NodeName = element(2, Member),
			 log("~s Stopping member ~p~n", [timestamp(), Member]),
                         catch erpc:call(NodeName, ra, stop_server, [?SYS, Member]),
                         catch peer:stop(PeerRef),
                         ok
                 end, Members),
    ok.

run_operations(State, _ClusterName) ->
    RemainingOps = maps:get(remaining_ops, State),
    case RemainingOps =< 0 of
        true ->
            State;
        false ->
            case RemainingOps rem 1000 of
                0 -> log("~s ~p operations remaining~n", [timestamp(), RemainingOps]);
                _ -> ok
            end,
            Operation = generate_operation(),
            NewState = execute_operation(State, Operation),

            % Update remaining operations count
            UpdatedState = NewState#{remaining_ops => RemainingOps - 1},

            % Validate consistency every 100 operations
            ValidationState = case maps:get(operations_count, UpdatedState) rem 100 of
                                  0 -> validate_consistency(UpdatedState);
                                  _ -> UpdatedState
                              end,

            run_operations(ValidationState, _ClusterName)
    end.

-spec generate_operation() -> operation().
generate_operation() ->
    case rand:uniform(100) of
        1 -> % 1% update almost all keys
            {update_almost_all_keys};
        2 -> % 1% add member
            {add_member};
        3 -> % 1% remove member
            {remove_member};
        4 -> % 1% kill WAL
            {kill_wal};
        5 -> % 1% kill member
            {kill_member};
        6 -> % 1% network partition
            {network_partition};
        N when N =< 10 -> % 4% snapshot
            {snapshot};
        N when N =< 12 -> % 2% major compactions
            {major_compaction};
        N when N =< 80 ->
            Key = generate_key(),
            Value = generate_value(),
            {put, Key, Value};
        _ ->
            Key = generate_key(),
            {get, Key}
    end.

key(N) when is_integer(N) ->
    <<"key_", (integer_to_binary(N))/binary>>.
generate_key() ->
    KeyNum = rand:uniform(?MAX_KEY), % Limited key space for more conflicts
    key(KeyNum).

-spec generate_value() -> binary().
generate_value() ->
    Size = rand:uniform(?MAX_VALUE_SIZE - ?MIN_VALUE_SIZE) + ?MIN_VALUE_SIZE,
    rand:bytes(Size).

-spec execute_operation(state(), operation()) -> state().
execute_operation(State, {put, Key, Value}) ->
    Members = maps:get(members, State),
    RefMap = maps:get(reference_map, State),
    OpCount = maps:get(operations_count, State),
    SuccessOps = maps:get(successful_ops, State),
    FailedOps = maps:get(failed_ops, State),

    % Pick a random cluster member to send the operation to
    MembersList = maps:keys(Members),
    Member = lists:nth(rand:uniform(length(MembersList)), MembersList),

    case ra_kv:put(Member, Key, Value, ?TIMEOUT) of
        {ok, _Meta} ->
            NewRefMap = RefMap#{Key => Value},
            State#{reference_map => NewRefMap,
                   operations_count => OpCount + 1,
                   successful_ops => SuccessOps + 1};
        {error, _Reason} ->
            State#{operations_count => OpCount + 1,
                   failed_ops => FailedOps + 1};
        {timeout, _ServerId} ->
            State#{operations_count => OpCount + 1,
                   failed_ops => FailedOps + 1}
    end;

execute_operation(State, {get, Key}) ->
    Members = maps:get(members, State),
    RefMap = maps:get(reference_map, State),
    OpCount = maps:get(operations_count, State),
    SuccessOps = maps:get(successful_ops, State),
    FailedOps = maps:get(failed_ops, State),

    % Pick a random cluster member to send the operation to
    MembersList = maps:keys(Members),
    Member = lists:nth(rand:uniform(length(MembersList)), MembersList),
    NodeName = element(2, Member),
    RefValue = maps:get(Key, RefMap, not_found),

    log("~s ra_kv:get/3 from node ~w ~n", [timestamp(), NodeName]),
    case erpc:call(NodeName, ra_kv, get, [Member, Key, ?TIMEOUT]) of
    % case apply(ra_kv, get, [Member, Key, ?TIMEOUT]) of
        {error, not_found} when RefValue =:= not_found ->
            State#{operations_count => OpCount + 1,
                   successful_ops => SuccessOps + 1};
        {error, not_found} when RefValue =/= not_found ->
            log("~s CONSISTENCY ERROR: Key ~p should exist but not found~n", [timestamp(), Key]),
            State#{operations_count => OpCount + 1,
                   failed_ops => FailedOps + 1};
        {ok, _Meta, Value} when RefValue =:= Value ->
            log("~s ra_kv:get/3 from node ~w ok! ~n", [timestamp(), NodeName]),
            State#{operations_count => OpCount + 1,
                   successful_ops => SuccessOps + 1};
        {ok, _Meta, Value} when RefValue =/= Value ->
            log("~s CONSISTENCY ERROR: Key ~p, Expected ~p, Got ~p~n",
                [timestamp(), Key, RefValue, Value]),
            State#{operations_count => OpCount + 1,
                   failed_ops => FailedOps + 1}
        % _ ->
        %     State#{operations_count => OpCount + 1,
        %            failed_ops => FailedOps + 1}
    end;

execute_operation(State, {update_almost_all_keys}) ->
    RefMap = maps:get(reference_map, State),
    OpCount = maps:get(operations_count, State),
    SuccessOps = maps:get(successful_ops, State),

    Member = random_non_partitioned_member(State),

    % Update a random percentage (10-90%) of keys
    PercentToUpdate = rand:uniform(81) + 9,  % Random number between 10 and 90
    AllKeys = lists:seq(1, ?MAX_KEY),
    NumKeysToUpdate = (?MAX_KEY * PercentToUpdate) div 100,
    KeysToUpdate = lists:sublist([N || {_, N} <- lists:sort([{rand:uniform(), K} || K <- AllKeys])],
                                  NumKeysToUpdate),

    {T, Results} = timer:tc(fun() ->
                               [begin
                                    case ra_kv:put(Member, key(N), OpCount, ?TIMEOUT) of
                                        {ok, _} -> {ok, N};
                                        {timeout, _} -> {timeout, N};
                                        {error, _Reason} -> {error, N}
                                    end
                                end || N <- KeysToUpdate]
                            end),

    % Count successful and failed updates
    SuccessfulKeys = [N || {ok, N} <- Results],
    FailedResults = [R || R <- Results, element(1, R) =/= ok],

    case FailedResults of
        [] ->
            log("~s Updated ~p% of the ~p keys (~p successful) in ~bms~n",
                [timestamp(), PercentToUpdate, ?MAX_KEY, length(SuccessfulKeys), T div 1000]);
        _ ->
            log("~s Updated ~p/~p keys (~p% target) in ~bms (aborted due to ~p failures/timeouts)~n",
                [timestamp(), length(SuccessfulKeys), length(KeysToUpdate), PercentToUpdate, T div 1000, length(FailedResults)])
    end,

    % Only update reference map for successfully updated keys
    NewRefMap = maps:merge(RefMap, maps:from_list([{key(N), OpCount} || N <- SuccessfulKeys])),
    State#{reference_map => NewRefMap,
           operations_count => OpCount + 1,
           successful_ops => SuccessOps + 1};

execute_operation(State, {snapshot}) ->
    Members = maps:get(members, State),

    % Pick a random cluster member to send snapshot command to
    MembersList = maps:keys(Members),
    Member = lists:nth(rand:uniform(length(MembersList)), MembersList),
    NodeName = element(2, Member),

    case erpc:call(NodeName, erlang, whereis, [?CLUSTER_NAME]) of
        undefined ->
            State;
        _Pid ->
            log("~s Rollover/snapshot on node ~p...~n", [timestamp(), NodeName]),
            erpc:call(NodeName, ra_log_wal, force_roll_over, [ra_log_wal]),
            erpc:call(NodeName, ra, aux_command, [Member, take_snapshot]),
            State
    end;

execute_operation(State, {major_compaction}) ->
    Members = maps:get(members, State),

    % Pick a random cluster member to send snapshot command to
    MembersList = maps:keys(Members),
    Member = lists:nth(rand:uniform(length(MembersList)), MembersList),
    NodeName = element(2, Member),

    case erpc:call(NodeName, erlang, whereis, [?CLUSTER_NAME]) of
        undefined ->
            State;
        _Pid ->
            log("~s Triggering major compaction on node ~p...~n", [timestamp(), NodeName]),
            erpc:call(NodeName, ra, trigger_compaction, [Member]),
            State
    end;

execute_operation(#{options := Opts} = State, {add_member}) ->
    Members = maps:get(members, State),
    OpCount = maps:get(operations_count, State),
    SuccessOps = maps:get(successful_ops, State),
    FailedOps = maps:get(failed_ops, State),
    NextNodeId = maps:get(next_node_id, State),

    % Don't add members if we already have 7 (maximum 7 nodes)
    case maps:size(Members) >= ?MAX_NODES of
        true ->
            State#{operations_count => OpCount + 1,
                   failed_ops => FailedOps + 1};
        false ->

            log("~s Adding member on node ~p.~n",
                [timestamp(), NextNodeId]),
    case start_new_peer_node(NextNodeId, Opts)  of
        {ok, PeerRef, NodeName} ->
            NewMember = {?CLUSTER_NAME, NodeName},
            ExistingMember = random_non_partitioned_member(State),

            case ra_kv:add_member(?SYS, NewMember, ExistingMember) of
                ok ->
                    NewMembers = Members#{NewMember => PeerRef},
                    NewMembersList = maps:keys(NewMembers),
                    log("~s Added member ~p. Cluster now has ~p members: ~0p; partitioned node: ~0p~n",
                        [timestamp(), NewMember,
                         length(NewMembersList),
                         NewMembersList,
                         partitioned_node_name(State)]),
                    State#{members => NewMembers,
                           operations_count => OpCount + 1,
                           successful_ops => SuccessOps + 1,
                           next_node_id => NextNodeId + 1};
                {timeout, _ServerId} ->
                    log("~s Timeout adding member ~p~n",
                        [timestamp(), NewMember]),
                    % Clean up the peer node since add failed
                    catch peer:stop(PeerRef),
                    State#{operations_count => OpCount + 1,
                           failed_ops => FailedOps + 1,
                           next_node_id => NextNodeId + 1};
                {error, Reason} ->
                    log("~s Failed to add member ~p: ~p~n",
                        [timestamp(), NewMember, Reason]),
                    % Clean up the peer node since add failed
                    catch peer:stop(PeerRef),
                    State#{operations_count => OpCount + 1,
                           failed_ops => FailedOps + 1,
                           next_node_id => NextNodeId + 1}
            end;
        {error, Reason} ->
            log("~s Failed to start peer node: ~p~n", [timestamp(), Reason]),
            State#{operations_count => OpCount + 1,
                   failed_ops => FailedOps + 1,
                   next_node_id => NextNodeId + 1}
    end
    end;

execute_operation(State, {remove_member}) ->
    Members = maps:get(members, State),
    OpCount = maps:get(operations_count, State),
    SuccessOps = maps:get(successful_ops, State),
    FailedOps = maps:get(failed_ops, State),

    % Don't remove members if we only have one left (minimum 1 node)
    case maps:size(Members) =< 1 of
        true ->
            State#{operations_count => OpCount + 1,
                   failed_ops => FailedOps + 1};
        false ->
            % Pick a random member to remove
            MembersList = maps:keys(Members),
            MemberToRemove = random_non_partitioned_member(State),

            % Pick a different member to send the remove command to
            RemainingMembers = MembersList -- [MemberToRemove],
            CommandTarget = random_non_partitioned_member(RemainingMembers, State),

            log("~s Removing member ~w... command target ~w~n",
                [timestamp(), MemberToRemove, CommandTarget]),

            case ra_kv:remove_member(?SYS, MemberToRemove, CommandTarget) of
                ok ->
                    % Stop the peer node for the removed member
                    case maps:get(MemberToRemove, Members, undefined) of
                        undefined ->
                            ok;
                        PeerRef ->
                            catch peer:stop(PeerRef)
                    end,

                    NewMembers = maps:remove(MemberToRemove, Members),
                    NewMembersList = maps:keys(NewMembers),
                    log("~s Member ~w removed. Cluster now has ~p members: ~0p; paritioned node: ~p~n",
                        [timestamp(),
                         MemberToRemove,
                         length(NewMembersList),
                         NewMembersList,
                         partitioned_node_name(State)]),

                    State#{members => NewMembers,
                           operations_count => OpCount + 1,
                           successful_ops => SuccessOps + 1};
                {timeout, _ServerId} ->
                    log("~s Timeout removing member ~p~n",
                        [timestamp(), MemberToRemove]),
                    State#{operations_count => OpCount + 1,
                           failed_ops => FailedOps + 1};
                {error, Reason} ->
                    log("~s Failed to remove member ~p: ~p~n",
                        [timestamp(), MemberToRemove, Reason]),
                    State#{operations_count => OpCount + 1,
                           failed_ops => FailedOps + 1}
            end
    end;

execute_operation(State, {kill_wal}) ->
    Members = maps:get(members, State),
    OpCount = maps:get(operations_count, State),
    SuccessOps = maps:get(successful_ops, State),

    % Pick a node to kill WAL on
    MembersList = maps:keys(Members),
    Rnd = rand:uniform(?MAX_NODES),
    case Rnd > length(MembersList) of
        true ->
            State#{operations_count => OpCount + 1,
                   successful_ops => SuccessOps + 1};
        false ->
            Member = lists:nth(Rnd, MembersList),
            NodeName = element(2, Member),

            log("~s Killing WAL on member ~w...~n", [timestamp(), NodeName]),

            case erpc:call(NodeName, erlang, whereis, [ra_log_wal]) of
                Pid when is_pid(Pid) ->
                    erpc:call(NodeName, erlang, exit, [Pid, kill]),
                    State#{operations_count => OpCount + 1,
                           successful_ops => SuccessOps + 1};
                _ ->
                    State
            end

    end;

execute_operation(State, {kill_member}) ->
    Members = maps:get(members, State),
    OpCount = maps:get(operations_count, State),
    SuccessOps = maps:get(successful_ops, State),
    Kills = maps:get(kills, State, #{}),

    % Pick a random member to kill
    MembersList = maps:keys(Members),
    Rnd = rand:uniform(?MAX_NODES),
    case Rnd > length(MembersList) of
        true ->
            State#{operations_count => OpCount + 1,
                   successful_ops => SuccessOps + 1};
        false ->
            Member = lists:nth(rand:uniform(length(MembersList)), MembersList),
            NodeName = element(2, Member),

            Now = milliseconds(),
            LastKill = maps:get(Member, Kills, Now - 10000),
            if(Now > LastKill + 5500) ->
                  case erpc:call(NodeName, erlang, whereis, [?CLUSTER_NAME]) of
                      Pid when is_pid(Pid) ->
                          log("~s Killing member ~w...~n", [timestamp(), Member]),
                          erpc:call(NodeName, erlang, exit, [Pid, kill]),
                          %% give it a bit of time after a kill in case this member is chosen
                          %% for the next operation
                          timer:sleep(100),
                          State#{operations_count => OpCount + 1,
                                 kills => Kills#{Member => Now},
                                 successful_ops => SuccessOps + 1};
                      _ ->
                          State
                  end;
              true ->
                  log("~s Not killing member ~w...~n", [timestamp(), Member]),
                  State
            end

    end;

execute_operation(State, {network_partition}) ->
    Members = maps:get(members, State),
    OpCount = maps:get(operations_count, State),
    SuccessOps = maps:get(successful_ops, State),
    PartitionState = maps:get(partition_state, State, #{}),
    Now = milliseconds(),

    case PartitionState of
        #{heal_time := HealTime} when Now < HealTime ->
            % There's an active partition already
            State;
        #{partitioned_node := PartitionedNode, heal_time := HealTime, other_nodes := OtherNodes} when Now >= HealTime ->
            % Time to heal the partition
            PartitionedNodeName = element(2, PartitionedNode),

            log("~s Healing network partition; node ~w will rejoin the rest~n", [timestamp(), PartitionedNode]),

            % Allow communication between partitioned node and other nodes
            % Call allow on each node to restore bidirectional communication
            % We use rpc:call with infinity timeout since nodes might be reconnecting
            [begin
                 spawn(fun() ->
                     case rpc:call(PartitionedNodeName, inet_tcp_proxy_dist, allow, [OtherNode], 5000) of
                         ok -> ok;
                         Err ->
                             log("~s rpc:call(~p, inet_tcp_proxy_dist, allow, [~p]) failed with ~p~n",
                                 [timestamp(),
                                  PartitionedNodeName,
                                  OtherNode,
                                  Err]),
                             ok
                     end
                 end),
                 spawn(fun() ->
                     case rpc:call(OtherNode, inet_tcp_proxy_dist, allow, [PartitionedNodeName], 5000) of
                         ok -> ok;
                         Err ->
                             log("~s rpc:call(~p, inet_tcp_proxy_dist, allow, [~p]) failed with ~p~n",
                                 [timestamp(),
                                  OtherNode,
                                  PartitionedNodeName,
                                  Err]),
                             ok
                     end
                 end)
             end || OtherNode <- OtherNodes],

            % Give nodes time to reconnect
            timer:sleep(5000),

            State#{operations_count => OpCount + 1,
                   successful_ops => SuccessOps + 1,
                   partition_state => #{}};
        _ ->
            MembersList = maps:keys(Members),
            case length(MembersList) < 3 of
                true ->
                    % Need at least 3 nodes for partition
                    State;
                false ->
                    % No active partition, create a new one
                    % Pick a random node to partition
                    NodeToPartition = lists:nth(rand:uniform(length(MembersList)), MembersList),
                    NodeToPartitionName = element(2, NodeToPartition),

                    % Get the other nodes (excluding the one to partition)
                    OtherNodes = [element(2, M) || M <- MembersList, M =/= NodeToPartition],

                    % Choose partition duration: 3s, 15s, 55s, or 90s
                    PartitionDurations = [3000, 15000, 55000, 90000],
                    Duration = lists:nth(rand:uniform(length(PartitionDurations)), PartitionDurations),
                    HealTime = Now + Duration,

                    log("~s Creating network partition: isolating node ~w from ~w for ~wms~n",
                        [timestamp(), NodeToPartitionName, OtherNodes, Duration]),

                    % Block communication between partitioned node and other nodes
                    % The harness node (current node) maintains access to all nodes
                    [begin
                         erpc:call(NodeToPartitionName, inet_tcp_proxy_dist, block, [OtherNode]),
                         erpc:call(OtherNode, inet_tcp_proxy_dist, block, [NodeToPartitionName])
                     end || OtherNode <- OtherNodes],

            State#{operations_count => OpCount + 1,
                   successful_ops => SuccessOps + 1,
                   partition_state => #{partitioned_node => NodeToPartition,
                                        heal_time => HealTime,
                                        other_nodes => OtherNodes}}
            end
    end.

-spec wait_for_applied_index_convergence([ra:server_id()], non_neg_integer()) -> ok.
wait_for_applied_index_convergence(Members, MaxRetries) when MaxRetries > 0 ->
    IndicesMap = get_applied_indices(Members),
    Indices = maps:values(IndicesMap),
    case lists:uniq(Indices) of
        [_SingleIndex] ->
            ok; % All nodes have converged
        _MultipleIndices ->
            timer:sleep(100), % Wait 100ms before retry
            wait_for_applied_index_convergence(Members, MaxRetries - 1)
    end;
wait_for_applied_index_convergence(Members, 0) ->
    IndicesMap = get_applied_indices(Members),
    log("~s WARNING: Applied index convergence timeout. Reported values: ~0p~n",
        [timestamp(), IndicesMap]),
    ok.

-spec get_applied_indices([ra:server_id()]) -> #{ra:server_id() => ra:index() | undefined}.
get_applied_indices(Members) ->
    maps:from_list([{Member, case ra:member_overview(Member, 1000) of
                                 {ok, #{last_applied := Index}, _} ->
                                     Index;
                                 _ ->
                                     undefined
                             end} || Member <- Members]).

-spec validate_consistency(state()) -> state().
validate_consistency(State) ->
    Members = maps:get(members, State),
    PartitionState = maps:get(partition_state, State, #{}),
    MembersList = maps:keys(Members),

    % Determine which members to validate based on active partition
    MembersToValidate = case PartitionState of
        #{partitioned_node := PartitionedNode} ->
            % Active partition - exclude the partitioned node
            log("~s Consistency check during partition; skipping validation of partitioned node ~0p~n",
                [timestamp(), partitioned_node_name(State)]),
            lists:delete(PartitionedNode, MembersList);
        _ ->
            % No active partition, validate all members
            MembersList
    end,

    % Perform consistency check on selected members
    perform_consistency_check(State, MembersToValidate).

-spec perform_consistency_check(state(), [ra:server_id()]) -> state().
perform_consistency_check(State, MembersToValidate) ->
    RefMap = maps:get(reference_map, State),

    % Wait for all nodes to converge to the same applied index
    wait_for_applied_index_convergence(MembersToValidate, 300), % Wait up to 30 seconds

    % Check that all members have the same view
    ValidationResults = [validate_member_consistency(Member, RefMap)
                         || Member <- MembersToValidate],

    Result1 = hd(ValidationResults),
    case lists:all(fun(Result) ->
                           is_map(Result) andalso
                           is_map(Result1) andalso
                           lists:sort(maps:get(live_indexes, Result)) =:=
                           lists:sort(maps:get(live_indexes, Result1))
                   end, ValidationResults) of
        true ->
            State;
        false ->
            % Brief console output with live_indexes summary
            LiveIndexesSummary = [{Member, case Result of
                                             #{live_indexes := LI,
                                               log := #{last_index := LastIndex}} ->
                                                   {length(LI), LastIndex};
                                             _ -> error
                                         end} || {Member, Result} <-
                                                 lists:zip(MembersToValidate, ValidationResults)],
            log("~s Consistency check failed. Live indexes per node: ~p~n",
                [timestamp(), LiveIndexesSummary]),
            log("~s STOPPING: No more operations will be performed due to consistency failure~n", [timestamp()]),

            % Write full details to log file with difference analysis
            LogEntry = format_consistency_failure(MembersToValidate, ValidationResults),
            file:write_file("ra_kv_harness.log", LogEntry, [append]),

            FailedOps = maps:get(failed_ops, State),
            State#{failed_ops => FailedOps + 1, remaining_ops => 0, consistency_failed => true}
    end.

-spec format_consistency_failure([ra:server_id()], [map() | error]) -> iolist().
format_consistency_failure(Members, Results) ->
    MemberResults = lists:zip(Members, Results),

    % Extract all unique results for comparison
    UniqueResults = lists:usort([R || {_, R} <- MemberResults, R =/= error]),

    Header = io_lib:format("~s Consistency check failed:~n", [timestamp()]),

    % Log raw data
    RawData = [io_lib:format("  Member ~p: ~p~n", [Member, Result]) || {Member, Result} <- MemberResults],

    % Analyze differences
    DiffAnalysis = case UniqueResults of
        [] ->
            ["  ANALYSIS: All members returned errors\n"];
        [_SingleResult] ->
            ["  ANALYSIS: All successful members have identical results (errors may exist)\n"];
        MultipleResults ->
            ["  ANALYSIS: Found ~p different result patterns:\n" |
             [io_lib:format("    Pattern ~p: ~p\n", [I, Pattern]) ||
              {I, Pattern} <- lists:zip(lists:seq(1, length(MultipleResults)), MultipleResults)] ++
             ["  DIFFERENCES:\n"] ++
             analyze_field_differences(MultipleResults)]
    end,

    [Header, RawData, DiffAnalysis, "\n"].

-spec analyze_field_differences([map()]) -> iolist().
analyze_field_differences(Results) ->
    % Extract live_indexes and num_keys for comparison
    LiveIndexes = [maps:get(live_indexes, R, undefined) || R <- Results, is_map(R)],
    NumKeys = [maps:get(num_keys, R, undefined) || R <- Results, is_map(R)],

    LiveIndexDiff = case lists:usort(LiveIndexes) of
        [_] -> [];
        MultipleLI -> [io_lib:format("    live_indexes differ: ~p\n", [MultipleLI])]
    end,

    NumKeysDiff = case lists:usort(NumKeys) of
        [_] -> [];
        MultipleNK -> [io_lib:format("    num_keys differ: ~p\n", [MultipleNK])]
    end,

    [LiveIndexDiff, NumKeysDiff].

-spec validate_member_consistency(ra:server_id(), map()) -> map() | error.
validate_member_consistency(Member, _RefMap) ->
    case ra_kv:member_overview(Member) of
        #{log := Log,
          machine := #{live_indexes := Live, num_keys := Num}} ->
            %io:format("Member ~p overview: Live indexes ~p, Num keys ~p", [Member, Live, Num]),
            #{log => Log,
              live_indexes => Live,
              num_keys => Num};
        Error ->
            log("~s Member ~p failed overview check: ~p~n",
                [timestamp(), Member, Error]),
            error
    end.

-spec validate_final_consistency(state()) -> non_neg_integer().
validate_final_consistency(State) ->
    Members = maps:get(members, State),
    RefMap = maps:get(reference_map, State),

    log("~s Performing final consistency validation...~n", [timestamp()]),
    log("~s Reference map has ~p keys~n", [timestamp(), maps:size(RefMap)]),

    % Wait for all nodes to converge before final validation
    MembersList = maps:keys(Members),
    log("~s Waiting for applied index convergence...~n", [timestamp()]),
    wait_for_applied_index_convergence(MembersList, 100), % Wait up to 10 seconds for final check

    % Validate all keys across all members
    Keys = maps:keys(RefMap),

    MembersList = maps:keys(Members),
    ValidationCount = lists:foldl(
                        fun(Key, Acc) ->
                                RefValue = maps:get(Key, RefMap),
                                case validate_key_across_members(Key, RefValue, MembersList) of
                                    ok -> Acc + 1;
                                    error -> Acc
                                end
                        end, 0, Keys),

    log("~s Final consistency check: ~p/~p keys validated successfully~n",
              [timestamp(), ValidationCount, length(Keys)]),
    ValidationCount.

-spec validate_key_across_members(binary(), term(), [ra:server_id()]) -> ok | error.
validate_key_across_members(Key, ExpectedValue, Members) ->
    Results = [begin
                   case erpc:call(Node, ra_kv, get, [Member, Key, ?TIMEOUT]) of
                       {ok, _Meta, Value} when Value =:= ExpectedValue -> ok;
                       {ok, _Meta, Value} ->
                           log("~s Key ~p mismatch on ~p: expected ~p, got ~p~n",
                                     [timestamp(), Key, Member, ExpectedValue, Value]),
                           error;
                       {error, not_found} ->
                           log("~s Key ~p not found on ~p but should exist~n", [timestamp(), Key, Member]),
                           error;
                       Other ->
                           log("~s Key ~p query failed on ~p: ~p~n", [timestamp(), Key, Member, Other]),
                           error
                   end
               end || {_, Node} = Member <- Members],

    case lists:all(fun(R) -> R =:= ok end, Results) of
        true -> ok;
        false -> error
    end.

partitioned_node_name(#{partitioned_state := #{partitioned_node := {_, Node}}}) ->
    Node;
partitioned_node_name(_State) ->
    none.

random_non_partitioned_member(MemberList, State) ->
    PartitionState = maps:get(partition_state, State, #{}),
    PartitionedNode = maps:get(partitioned_node, PartitionState, none),
    MajorityMembers = lists:delete(PartitionedNode, MemberList),
    lists:nth(rand:uniform(length(MajorityMembers)), MajorityMembers).

random_non_partitioned_member(State) ->
    Members = maps:get(members, State, #{}),
    PartitionState = maps:get(partition_state, State, #{}),
    PartitionedNode = maps:get(partitioned_node, PartitionState, none),
    MajorityMembers = lists:delete(PartitionedNode, maps:keys(Members)),
    lists:nth(rand:uniform(length(MajorityMembers)), MajorityMembers).

