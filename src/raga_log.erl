-module(raga_log).

-include("raga.hrl").

-type raga_log_init_args() :: [term()].
-type raga_log_state() :: {raga_index(), any()}.


-callback init(Args::raga_log_init_args()) -> raga_log_state().

-callback append(Entry::log_entry(), Overwrite::boolean(),
                 State::raga_log_state()) ->
    {ok, raga_log_state()} | {error, integrity_error}.

-callback take(Start::raga_index(), Num::non_neg_integer(),
               State::raga_log_state()) ->
    [log_entry()].

-callback fetch(Inx::raga_index(), State::raga_log_state()) ->
    maybe(log_entry()).

-callback last(State::raga_log_state()) ->
    maybe(log_entry()).
