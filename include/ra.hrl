-type maybe(T) :: undefined | T.

-type ra_index() :: non_neg_integer().
-type ra_term() :: non_neg_integer().

% NB: ra nodes need to be registered as need to be reachable under the old
% name after restarts. Pids won't do.
-type ra_node_id() :: Name::atom() | {Name::atom(), Node::atom()} |
                        {global, Name::atom()}.
-type log_entry() :: {ra_index(), ra_term(), term()}.


-record(append_entries_rpc,
        {term :: ra_term(),
         leader_id :: ra_node_id(),
         prev_log_index :: non_neg_integer(),
         prev_log_term :: ra_term(),
         entries = [] :: [log_entry()],
         leader_commit :: ra_index()}).

% TODO: optimisation - follower could send last committ indx when
% success is false to allow leader to skip to that index
% rather than incrementally send append_entries_rpcs
-record(append_entries_reply,
        {term :: ra_term(),
         success :: boolean(),
         % because we aren't doing true rpc we may have multiple append
         % entries in flight we need to communicate what we are replying
         % to
         last_index :: ra_index(),
         last_term :: ra_term()}).

-record(request_vote_rpc,
        {term :: ra_term(),
         candidate_id :: ra_node_id(),
         last_log_index :: ra_index(),
         last_log_term :: ra_index()}).

-record(request_vote_result,
        {term :: ra_term(),
         vote_granted :: boolean()}).

-type ra_msg() :: #append_entries_rpc{} | #append_entries_reply{}.

-type ra_action() :: {reply, ra_msg()} |
                        {vote | append,
                         [{ra_node_id(), ra_msg()}]} |
                       [ra_action()] | none.

-define(DBG(Fmt, Args), error_logger:info_msg(Fmt, Args)).

