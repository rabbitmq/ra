-module(ra_log_meta).

-export([init/1
         % store/3,
         % fetch/2,
         % sync/1
        ]).

-type key() :: current_term | voted_for | last_applied.
-type value() :: non_neg_integer() | atom() | {atom(), atom()}.

-define(HEADER_SIZE, 8).
-define(CURRENT_TERM_OFFS, 8).
-define(CURRENT_TERM_SIZE, 8).
-define(LAST_APPLIED_OFFS, 16).
-define(LAST_APPLIED_SIZE, 8).
-define(VOTED_FOR_OFFS, 24).
-define(VOTED_FOR_SIZE, 512).

-record(state, {fd :: file:fd(),
                data = #{} :: #{key() => value()}}).

init(Fn) ->
    ok = filelib:ensure_dir(Fn),
    {ok, Fd} = file:open(Fn, [binary, raw, read, write]),
    #state{fd = Fd}.




