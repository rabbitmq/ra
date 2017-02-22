-type maybe(T) :: undefined | T.

-type raga_index() :: non_neg_integer().
-type raga_term() :: non_neg_integer().

% NB: raga nodes need to be registered as need to be reachable under the old
% name after restarts. Pids won't do.
-type raga_node_id() :: Name::atom() | {Name::atom(), Node::atom()} |
                        {global, Name::atom()}.
-type log_entry() :: {raga_index(), raga_term(), term()}.


-record(append_entries_rpc,
        {term :: raga_term(),
         leader_id :: raga_node_id(),
         prev_log_index :: non_neg_integer(),
         prev_log_term :: raga_term(),
         entries = [] :: [log_entry()],
         leader_commit :: raga_index()}).

% TODO: optimisation - follower could send last committ indx when
% success is false to allow leader to skip to that index
% rather than incrementally send append_entries_rpcs
-record(append_entries_reply,
        {term :: raga_term(),
         success :: boolean()}).

-record(request_vote_rpc,
        {term :: raga_term(),
         candidate_id :: raga_node_id(),
         last_log_index :: raga_index(),
         last_log_term :: raga_index()}).

-record(request_vote_result,
        {term :: raga_term(),
         vote_granted :: boolean()}).

-type raga_msg() :: #append_entries_rpc{} | #append_entries_reply{}.

-type raga_action() :: {reply, raga_msg()} |
                        {vote | append,
                         [{raga_node_id(), raga_msg()}]} | none.

-define(DBG(Fmt, Args), error_logger:info_msg(Fmt, Args)).

