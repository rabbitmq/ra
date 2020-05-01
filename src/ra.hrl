-type maybe(T) :: undefined | T.

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
-type ra_server_id() :: atom() | {Name :: atom(), Node :: node()}.

-type ra_peer_status() :: normal | {sending_snapshot, pid()}.

-type ra_peer_state() :: #{next_index := non_neg_integer(),
                           match_index := non_neg_integer(),
                           query_index := non_neg_integer(),
                           % the commit index last sent
                           % used for evaluating pipeline status
                           commit_index_sent := non_neg_integer(),
                           %% indicates that a snapshot is being sent
                           %% to the peer
                           status => ra_peer_status()}.

-type ra_cluster() :: #{ra_server_id() => ra_peer_state()}.

-type ra_cluster_servers() :: [ra_server_id()].

%% represent a unique entry in the ra log
-type log_entry() :: {ra_index(), ra_term(), term()}.

-type chunk_flag() :: next | last.

-type consistent_query_ref() :: {From :: term(), Query :: ra:query_fun(), ConmmitIndex :: ra_index()}.

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
         last_log_term :: ra_index()}).

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
         last_log_term :: ra_index()}).

-record(pre_vote_result,
        {term :: ra_term(),
         token :: reference(),
         vote_granted :: boolean()}).

-type snapshot_meta() :: #{index := ra_index(),
                           term := ra_term(),
                           cluster := ra_cluster_servers(),
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

%% WAL defaults
-define(WAL_DEFAULT_MAX_SIZE_BYTES, 256 * 1000 * 1000).
-define(WAL_DEFAULT_MAX_BATCH_SIZE, 8192).
%% define a minimum allowable wal size. If anyone tries to set a really small
%% size that is smaller than the logical block size the pre-allocation code may
%% fail
-define(WAL_MIN_SIZE, 65536).
%% The size of each WAL file chunk that is processed at a time during recovery
-define(WAL_RECOVERY_CHUNK_SIZE, 33554432).

%% logging shim
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
                                                        line => ?LINE}),
       ok).

-define(DEFAULT_TIMEOUT, 5000).

-define(DEFAULT_SNAPSHOT_MODULE, ra_log_snapshot).
