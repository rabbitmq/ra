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

%% uniquely identifies the ra cluster
-type ra_cluster_id() :: term().

%% Uniquely identifies a ra node on a local erlang node
%% used for on disk resources and local name to pid mapping
-type ra_uid() :: binary().

%% Identifies a ra node in a ra cluster
%% NB: ra nodes need to be registered as need to be reachable under the old
%% name after restarts. Pids won't do.
-type ra_node_id() :: atom() | {Name :: atom(), Node :: node()}.

-type ra_peer_state() :: #{next_index => non_neg_integer(),
                           match_index => non_neg_integer(),
                           % the commit index last sent
                           % used for evaluating pipeline status
                           commit_index => non_neg_integer()}.

-type ra_cluster() :: #{ra_node_id() => ra_peer_state()}.

%% represent a unique entry in the ra log
-type log_entry() :: {ra_index(), ra_term(), term()}.

%% Figure 2 in the paper
-record(append_entries_rpc,
        {term :: ra_term(),
         leader_id :: ra_node_id(),
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
         candidate_id :: ra_node_id(),
         last_log_index :: ra_index(),
         last_log_term :: ra_index()}).

%% Section 4.2
-record(request_vote_result,
        {term :: ra_term(),
         vote_granted :: boolean()}).

%% pre-vote extension
-record(pre_vote_rpc,
        {term :: ra_term(),
         candidate_id :: ra_node_id(),
         last_log_index :: ra_index(),
         last_log_term :: ra_index()}).

-record(pre_vote_result,
        {term :: ra_term(),
         vote_granted :: boolean()}).

-record(install_snapshot_rpc,
        {term :: ra_term(), % the leaders term
         leader_id :: ra_node_id(),
         last_index :: ra_index(), % the snapshot replaces all previous entries incl this
         last_term :: ra_term(), % the term at the point of snapshot
         last_config :: ra_cluster(),
         % because we only snapshot when the state is effectively empty
         % we should never need to create chunks
         data :: term()
        }).

-record(install_snapshot_result,
        {term :: ra_term(),
         % because we aren't doing true rpc we may have multiple append
         % entries in flight we need to communicate what we are replying
         % to
         last_index :: ra_index(),
         last_term :: ra_term()}).


% primitive logging abstraction
-define(error, true).
-define(warn, true).
% -define(info, true).

-ifdef(info).
-define(INFO(Fmt, Args), error_logger:info_msg(Fmt, Args)).
-else.
-define(INFO(_F, _A), ok).
-endif.

-ifdef(warn).
-define(WARN(Fmt, Args), error_logger:warning_msg(Fmt, Args)).
-else.
-define(WARN(_, _), ok).
-endif.

-ifdef(error).
-define(ERR(Fmt, Args), error_logger:error_msg(Fmt, Args)).
-else.
-define(ERR(_, _), ok).
-endif.

-define(DEFAULT_TIMEOUT, 5000).

