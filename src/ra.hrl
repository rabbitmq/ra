%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%%
-type option(T) :: undefined | T.

%%
%% Most of the records here are covered on Figure 2
%% in the Raft paper (extended version):
%% https://raft.github.io/raft.pdf.
%%

%% taken from gen_statem as this type isn't exported for some reason.
-type from() ::
	{To :: pid(), Tag :: term()}.

%% Sections 5.1 in the paper.
-type ra_index() :: non_neg_integer().
%% Section 5.3.
-type ra_term() :: non_neg_integer().

%% tuple form of index and term
-type ra_idxterm() :: {ra_index(), ra_term()}.

%% Sections 5.1-5.3.

%% a friendly name for the cluster
-type ra_cluster_name() :: binary() | string() | atom().

%% Uniquely identifies a ra server on a local erlang node
%% used for on disk resources and local name to pid mapping
-type ra_uid() :: binary().

%% Identifies a Ra server (node) in a Ra cluster.
%%
%% Ra servers need to be registered stable names (names that are reachable
%% after node restart). Pids are not stable in this sense.
-type ra_server_id() :: {Name :: atom(), Node :: node()}.

%% Specifies server configuration for a new cluster member.
%% Subset of  ra_server:ra_server_config().
%% Both `ra:add_member` and `ra:start_server` must be called with the same values.
-type ra_new_server() :: #{id := ra_server_id(),
                           % Defaults to `voter` if absent.
                           membership => ra_membership(),
                           % Required for `promotable` in the above.
                           uid => ra_uid()}.

-type ra_peer_status() :: normal |
                          {sending_snapshot, pid()} |
                          suspended |
                          disconnected.

-type ra_membership() :: voter | promotable | non_voter | unknown.

-type ra_voter_status() :: #{membership => ra_membership(),
                             uid => ra_uid(),
                             target => ra_index()}.

-type ra_peer_state() :: #{next_index := non_neg_integer(),
                           match_index := non_neg_integer(),
                           query_index := non_neg_integer(),
                           % the commit index last sent
                           % used for evaluating pipeline status
                           commit_index_sent := non_neg_integer(),
                           %% Whether the peer is part of the consensus.
                           %% Defaults to "yes" if absent.
                           voter_status => ra_voter_status(),
                           %% indicates that a snapshot is being sent
                           %% to the peer
                           status := ra_peer_status(),
                           machine_version => ra_machine:version()}.

-type ra_cluster() :: #{ra_server_id() => ra_peer_state()}.

%% Dehydrated cluster:
-type ra_cluster_servers() :: [ra_server_id()].  % Deprecated
-type ra_peer_snapshot() :: #{voter_status => ra_voter_status()}.
-type ra_cluster_snapshot() :: #{ra_server_id() => ra_peer_snapshot()}.

%% represent a unique entry in the ra log
-type log_entry() :: {ra_index(), ra_term(), term()}.

-type chunk_flag() :: next | last.

-type consistent_query_ref() :: {From :: term(), Query :: ra:query_fun(), ConmmitIndex :: ra_index()}.

-type safe_call_ret(T) :: timeout | {error, noproc | nodedown | shutdown} | T.

-type states() :: leader | follower | candidate | await_condition.

%% A member of the cluster from which replies should be sent.
-type ra_reply_from() :: leader | local | {member, ra_server_id()}.

-define(RA_PROTO_VERSION, 1).
%% the protocol version should be incremented whenever extensions need to be
%% done to the core protocol records (below). It is only ever exchanged by the
%% pre_vote message so that servers can reject pre votes for servers running a
%% higher protocol version. This should be sufficient to disallow a server with
%% newer protocol version to become leader before it has a majority and thus
%% potentially exchange incompatible message types with servers running older
%% code.
%%
%% If fields need to be added to any of the below records it is suggested that
%% a new record is created (by appending _vPROTO_VERSION). The server still need
%% to be able to handle and reply to the older message types to ensure
%% availability.

%% Figure 2 in the paper
-record(append_entries_rpc,
        {term :: ra_term(),
         leader_id :: ra_server_id(),
         leader_commit :: ra_index(),
         prev_log_index :: non_neg_integer(),
         prev_log_term :: ra_term(),
         entries = [] :: [log_entry()]}).

-record(append_entries_reply,
        {term :: ra_term(),
         success :: boolean(),
         % because we aren't doing true rpc we may have multiple append
         % entries in flight we need to communicate what we are replying
         % to
         % because writes are fsynced asynchronously we need to indicate
         % the last index seen as well as the last index persisted.
         next_index :: ra_index(),
         % the last index that has been fsynced to disk
         last_index :: ra_index(),
         last_term :: ra_term()}).

%% Section 5.2
-record(request_vote_rpc,
        {term :: ra_term(),
         candidate_id :: ra_server_id(),
         last_log_index :: ra_index(),
         last_log_term :: ra_term()}).

%% Section 4.2
-record(request_vote_result,
        {term :: ra_term(),
         vote_granted :: boolean()}).

%% pre-vote extension
-record(pre_vote_rpc,
        {version = ?RA_PROTO_VERSION :: non_neg_integer(),
         %% servers will only vote for servers with a matching machine_version
         machine_version :: non_neg_integer(),
         term :: ra_term(),
         token :: reference(),
         candidate_id :: ra_server_id(),
         last_log_index :: ra_index(),
         last_log_term :: ra_term()}).

-record(pre_vote_result,
        {term :: ra_term(),
         token :: reference(),
         vote_granted :: boolean()}).

-type snapshot_meta() :: #{index := ra_index(),
                           term := ra_term(),
                           cluster := ra_cluster_snapshot(),
                           machine_version := ra_machine:version()}.

-record(install_snapshot_rpc,
        {term :: ra_term(), % the leader's term
         leader_id :: ra_server_id(),
         meta :: snapshot_meta(),
         chunk_state :: {pos_integer(), chunk_flag()} | undefined,
         data :: term()
        }).

-record(install_snapshot_result,
        {term :: ra_term(),
         % because we need to inform the leader of the snapshot that has been
         % replicated from another process we here include the index and
         % term of the snapshot in question
         last_index :: ra_index(),
         last_term :: ra_term()}).

-record(heartbeat_rpc,
        {query_index :: integer(),
         term :: ra_term(),
         leader_id :: ra_server_id()}).

-record(heartbeat_reply,
        {query_index :: integer(),
         term :: ra_term()}).

-record(info_rpc,
        {from :: ra_server_id(),
         term :: ra_term(),
         keys :: [ra_server:ra_server_info_key()]}).

-record(info_reply,
        {from :: ra_server_id(),
         term :: ra_term(),
         keys :: [ra_server:ra_server_info_key()],
         info = #{} :: ra_server:ra_server_info()}).

%% WAL defaults
-define(WAL_DEFAULT_MAX_SIZE_BYTES, 256 * 1000 * 1000).
-define(WAL_DEFAULT_MAX_BATCH_SIZE, 8192).
-define(MIN_BIN_VHEAP_SIZE, 46422).
-define(MIN_HEAP_SIZE, 233).
%% define a minimum allowable wal size. If anyone tries to set a really small
%% size that is smaller than the logical block size the pre-allocation code may
%% fail
-define(WAL_MIN_SIZE, 65536).
%% The size of each WAL file chunk that is processed at a time during recovery
-define(WAL_RECOVERY_CHUNK_SIZE, 33554432).
%% segment defaults
-define(SEGMENT_MAX_ENTRIES, 4096).
-define(SEGMENT_MAX_PENDING, 1024).
-define(SEGMENT_MAX_SIZE_B, 64_000_000). %% set an upper limit on segment sizing

%% logging shim
-define(DEBUG_IF(Bool, Fmt, Args),
        case Bool of
            true -> ?DISPATCH_LOG(debug, Fmt, Args);
            false -> ok
        end).
-define(DEBUG(Fmt, Args), ?DISPATCH_LOG(debug, Fmt, Args)).
-define(INFO(Fmt, Args), ?DISPATCH_LOG(info, Fmt, Args)).
-define(NOTICE(Fmt, Args), ?DISPATCH_LOG(notice, Fmt, Args)).
-define(WARN(Fmt, Args), ?DISPATCH_LOG(warning, Fmt, Args)).
-define(WARNING(Fmt, Args), ?DISPATCH_LOG(warning, Fmt, Args)).
-define(ERR(Fmt, Args), ?DISPATCH_LOG(error, Fmt, Args)).
-define(ERROR(Fmt, Args), ?DISPATCH_LOG(error, Fmt, Args)).

-define(DISPATCH_LOG(Level, Fmt, Args),
        %% same as OTP logger does when using the macro
        catch (persistent_term:get('$ra_logger')):log(Level, Fmt, Args,
                                                      #{mfa => {?MODULE,
                                                                ?FUNCTION_NAME,
                                                                ?FUNCTION_ARITY},
                                                        file => ?FILE,
                                                        line => ?LINE,
                                                        domain => [ra]}),
       ok).

-define(DEFAULT_TIMEOUT, 5000).

-define(DEFAULT_SNAPSHOT_MODULE, ra_log_snapshot).

-define(DEFAULT_MAX_CHECKPOINTS, 10).

-define(RA_LOG_COUNTER_FIELDS,
        [{write_ops, ?C_RA_LOG_WRITE_OPS, counter,
          "Total number of write ops"},
         {write_resends, ?C_RA_LOG_WRITE_RESENDS, counter,
          "Total number of write resends"},
         {read_ops, ?C_RA_LOG_READ_OPS, counter,
          "Total number of read ops"},
         {read_cache, ?C_RA_LOG_READ_CACHE, counter,
          "Unused. Total number of cache reads"},
         {read_mem_table, ?C_RA_LOG_READ_MEM_TBL, counter,
          "Total number of reads from mem tables"},
         {read_closed_mem_tbl, ?C_RA_LOG_READ_CLOSED_MEM_TBL, counter,
          "Unused. Total number of closed memory tables"},
         {read_segment, ?C_RA_LOG_READ_SEGMENT, counter,
          "Total number of read segments"},
         {fetch_term, ?C_RA_LOG_FETCH_TERM, counter,
          "Total number of terms fetched"},
         {snapshots_written, ?C_RA_LOG_SNAPSHOTS_WRITTEN, counter,
          "Total number of snapshots written"},
         {snapshot_installed, ?C_RA_LOG_SNAPSHOTS_INSTALLED, counter,
          "Total number of snapshots installed"},
         {snapshot_bytes_written, ?C_RA_LOG_SNAPSHOT_BYTES_WRITTEN, counter,
          "Number of snapshot bytes written (not installed)"},
         {open_segments, ?C_RA_LOG_OPEN_SEGMENTS, gauge, "Number of open segments"},
         {checkpoints_written, ?C_RA_LOG_CHECKPOINTS_WRITTEN, counter,
          "Total number of checkpoints written"},
         {checkpoint_bytes_written, ?C_RA_LOG_CHECKPOINT_BYTES_WRITTEN, counter,
          "Number of checkpoint bytes written"},
         {checkpoints_promoted, ?C_RA_LOG_CHECKPOINTS_PROMOTED, counter,
          "Number of checkpoints promoted to snapshots"},
         {reserved_1, ?C_RA_LOG_RESERVED, counter, "Reserved counter"}
         ]).
-define(C_RA_LOG_WRITE_OPS, 1).
-define(C_RA_LOG_WRITE_RESENDS, 2).
-define(C_RA_LOG_READ_OPS, 3).
-define(C_RA_LOG_READ_CACHE, 4).
-define(C_RA_LOG_READ_MEM_TBL, 5).
-define(C_RA_LOG_READ_CLOSED_MEM_TBL, 6).
-define(C_RA_LOG_READ_SEGMENT, 7).
-define(C_RA_LOG_FETCH_TERM, 8).
-define(C_RA_LOG_SNAPSHOTS_WRITTEN, 9).
-define(C_RA_LOG_SNAPSHOTS_INSTALLED, 10).
-define(C_RA_LOG_SNAPSHOT_BYTES_WRITTEN, 11).
-define(C_RA_LOG_OPEN_SEGMENTS, 12).
-define(C_RA_LOG_CHECKPOINTS_WRITTEN, 13).
-define(C_RA_LOG_CHECKPOINT_BYTES_WRITTEN, 14).
-define(C_RA_LOG_CHECKPOINTS_PROMOTED, 15).
-define(C_RA_LOG_RESERVED, 16).

-define(C_RA_SRV_AER_RECEIVED_FOLLOWER, ?C_RA_LOG_RESERVED + 1).
-define(C_RA_SRV_AER_REPLIES_SUCCESS, ?C_RA_LOG_RESERVED + 2).
-define(C_RA_SRV_AER_REPLIES_FAILED, ?C_RA_LOG_RESERVED + 3).
-define(C_RA_SRV_COMMANDS, ?C_RA_LOG_RESERVED + 4).
-define(C_RA_SRV_COMMAND_FLUSHES, ?C_RA_LOG_RESERVED + 5).
-define(C_RA_SRV_AUX_COMMANDS, ?C_RA_LOG_RESERVED + 6).
-define(C_RA_SRV_CONSISTENT_QUERIES, ?C_RA_LOG_RESERVED + 7).
-define(C_RA_SRV_RPCS_SENT, ?C_RA_LOG_RESERVED + 8).
-define(C_RA_SRV_MSGS_SENT, ?C_RA_LOG_RESERVED + 9).
-define(C_RA_SRV_DROPPED_SENDS, ?C_RA_LOG_RESERVED + 10).
-define(C_RA_SRV_SEND_MSG_EFFS_SENT, ?C_RA_LOG_RESERVED + 11).
-define(C_RA_SRV_PRE_VOTE_ELECTIONS, ?C_RA_LOG_RESERVED + 12).
-define(C_RA_SRV_ELECTIONS, ?C_RA_LOG_RESERVED + 13).
-define(C_RA_SRV_GCS, ?C_RA_LOG_RESERVED + 14).
-define(C_RA_SRV_SNAPSHOTS_SENT, ?C_RA_LOG_RESERVED + 15).
-define(C_RA_SRV_RELEASE_CURSORS, ?C_RA_LOG_RESERVED + 16).
-define(C_RA_SRV_AER_RECEIVED_FOLLOWER_EMPTY, ?C_RA_LOG_RESERVED + 17).
-define(C_RA_SRV_TERM_AND_VOTED_FOR_UPDATES, ?C_RA_LOG_RESERVED + 18).
-define(C_RA_SRV_LOCAL_QUERIES, ?C_RA_LOG_RESERVED + 19).
-define(C_RA_SRV_INVALID_REPLY_MODE_COMMANDS, ?C_RA_LOG_RESERVED + 20).
-define(C_RA_SRV_CHECKPOINTS, ?C_RA_LOG_RESERVED + 21).
-define(C_RA_SRV_RESERVED, ?C_RA_LOG_RESERVED + 22).


-define(RA_SRV_COUNTER_FIELDS,
        [
         {aer_received_follower, ?C_RA_SRV_AER_RECEIVED_FOLLOWER, counter,
          "Total number of append entries received by a follower"},
         {aer_replies_success, ?C_RA_SRV_AER_REPLIES_SUCCESS, counter,
          "Total number of successful append entries received"},
         {aer_replies_fail, ?C_RA_SRV_AER_REPLIES_FAILED, counter,
          "Total number of failed append entries"},
         {commands, ?C_RA_SRV_COMMANDS, counter,
          "Total number of commands received by a leader"},
         {command_flushes, ?C_RA_SRV_COMMAND_FLUSHES, counter,
          "Total number of low priority command batches written"},
         {aux_commands, ?C_RA_SRV_AUX_COMMANDS, counter,
          "Total number of aux commands received"},
         {consistent_queries, ?C_RA_SRV_CONSISTENT_QUERIES, counter,
          "Total number of consistent query requests"},
         {rpcs_sent, ?C_RA_SRV_RPCS_SENT, counter,
          "Total number of rpcs, incl append_entries_rpcs"},
         {msgs_sent, ?C_RA_SRV_MSGS_SENT, counter,
          "All messages sent (except messages sent to wal)"},
         {dropped_sends, ?C_RA_SRV_DROPPED_SENDS, counter,
          "Total number of message sends that return noconnect or nosuspend are dropped"},
         {send_msg_effects_sent, ?C_RA_SRV_SEND_MSG_EFFS_SENT, counter,
          "Total number of send_msg effects executed"},
         {pre_vote_elections, ?C_RA_SRV_PRE_VOTE_ELECTIONS, counter,
          "Total number of pre-vote elections"},
         {elections, ?C_RA_SRV_ELECTIONS, counter,
          "Total number of elections"},
         {forced_gcs, ?C_RA_SRV_GCS, counter,
          "Number of forced garbage collection runs"},
         {snapshots_sent, ?C_RA_SRV_SNAPSHOTS_SENT, counter,
          "Total number of snapshots sent"},
         {release_cursors, ?C_RA_SRV_RELEASE_CURSORS, counter,
          "Total number of updates of the release cursor"},
         {aer_received_follower_empty, ?C_RA_SRV_AER_RECEIVED_FOLLOWER_EMPTY, counter,
          "Total number of empty append entries received by a follower"},
         {term_and_voted_for_updates, ?C_RA_SRV_TERM_AND_VOTED_FOR_UPDATES, counter,
          "Total number of updates of term and voted for"},
         {local_queries, ?C_RA_SRV_LOCAL_QUERIES, counter,
          "Total number of local queries"},
         {invalid_reply_mode_commands, ?C_RA_SRV_INVALID_REPLY_MODE_COMMANDS, counter,
          "Total number of commands received with an invalid reply-mode"},
         {checkpoints, ?C_RA_SRV_CHECKPOINTS, counter,
          "The number of checkpoint effects executed"},
         {reserved_2, ?C_RA_SRV_RESERVED, counter, "Reserved counter"}
         ]).

-define(C_RA_SVR_METRIC_LAST_APPLIED, ?C_RA_SRV_RESERVED + 1).
-define(C_RA_SVR_METRIC_COMMIT_INDEX, ?C_RA_SRV_RESERVED + 2).
-define(C_RA_SVR_METRIC_SNAPSHOT_INDEX, ?C_RA_SRV_RESERVED + 3).
-define(C_RA_SVR_METRIC_LAST_INDEX, ?C_RA_SRV_RESERVED + 4).
-define(C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, ?C_RA_SRV_RESERVED + 5).
-define(C_RA_SVR_METRIC_COMMIT_LATENCY, ?C_RA_SRV_RESERVED + 6).
-define(C_RA_SVR_METRIC_TERM, ?C_RA_SRV_RESERVED + 7).
-define(C_RA_SVR_METRIC_CHECKPOINT_INDEX, ?C_RA_SRV_RESERVED + 8).
-define(C_RA_SVR_METRIC_EFFECTIVE_MACHINE_VERSION, ?C_RA_SRV_RESERVED + 9).
-define(C_RA_SVR_METRIC_NUM_SEGMENTS, ?C_RA_SRV_RESERVED + 10).

-define(RA_SRV_METRICS_COUNTER_FIELDS,
        [
         {last_applied, ?C_RA_SVR_METRIC_LAST_APPLIED, gauge,
          "The last applied index. Can go backwards if a ra server is restarted."},
         {commit_index, ?C_RA_SVR_METRIC_COMMIT_INDEX, counter,
          "The current commit index."},
         {snapshot_index, ?C_RA_SVR_METRIC_SNAPSHOT_INDEX, counter,
          "The current snapshot index."},
         {last_index, ?C_RA_SVR_METRIC_LAST_INDEX, counter,
          "The last index of the log."},
         {last_written_index, ?C_RA_SVR_METRIC_LAST_WRITTEN_INDEX, counter,
          "The last fully written and fsynced index of the log."},
         {commit_latency, ?C_RA_SVR_METRIC_COMMIT_LATENCY, {gauge, time_ms},
          "Approximate time taken from an entry being written to the log until it is committed."},
         {term, ?C_RA_SVR_METRIC_TERM, counter, "The current term."},
         {checkpoint_index, ?C_RA_SVR_METRIC_CHECKPOINT_INDEX, counter,
          "The current checkpoint index."},
         {effective_machine_version, ?C_RA_SVR_METRIC_EFFECTIVE_MACHINE_VERSION,
          gauge, "The current effective version number of the machine."},
         {num_segments, ?C_RA_SVR_METRIC_NUM_SEGMENTS,
          gauge, "The number of non-empty segment files."}
        ]).

-define(RA_COUNTER_FIELDS,
        ?RA_LOG_COUNTER_FIELDS ++
        ?RA_SRV_COUNTER_FIELDS ++
        ?RA_SRV_METRICS_COUNTER_FIELDS).

-define(FIELDSPEC_KEY, ra_seshat_fields_spec).
