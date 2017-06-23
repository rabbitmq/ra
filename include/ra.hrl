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
%%
%% Uniquely identifies a ra node
%% NB: ra nodes need to be registered as need to be reachable under the old
%% name after restarts. Pids won't do.
-type ra_node_id() :: Name::atom() | {Name::atom(), Node::atom()} |
                      {global, Name::atom()}.

-type ra_peer_state() :: #{next_index => non_neg_integer(),
                           match_index => non_neg_integer()}.

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

%% TODO: optimisation - follower could send last commit index when
%% success is false to allow leader to skip to that index
%% rather than incrementally send append_entries_rpcs
-record(append_entries_reply,
        {term :: ra_term(),
         success :: boolean(),
         % because we aren't doing true rpc we may have multiple append
         % entries in flight we need to communicate what we are replying
         % to
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


%% similar to install snapshot rpc but communicates a reset index
%% at which point all preceeding entries adds upp to the initial machine
%% state
-record(reset_rpc,
        {index :: ra_index(), % the index to reset log to
         term :: ra_term(),
         leader_id :: ra_node_id(),
         % the cluster at the time of the reset index
         current_cluster :: {ra_index(), ra_cluster()}
        }).

-define(DBG(Fmt, Args), error_logger:info_msg(Fmt, Args)).

-define(DEFAULT_TIMEOUT, 5000).


