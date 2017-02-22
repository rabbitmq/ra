-module(ra_log).

-include("ra.hrl").

-type ra_log_init_args() :: [term()].
-type ra_log_state() :: {ra_index(), any()}.


-callback init(Args::ra_log_init_args()) -> ra_log_state().

-callback append(Entry::log_entry(), Overwrite::boolean(),
                 State::ra_log_state()) ->
    {ok, ra_log_state()} | {error, integrity_error}.

-callback take(Start::ra_index(), Num::non_neg_integer(),
               State::ra_log_state()) ->
    [log_entry()].

-callback fetch(Inx::ra_index(), State::ra_log_state()) ->
    maybe(log_entry()).

-callback last(State::ra_log_state()) ->
    maybe(log_entry()).
