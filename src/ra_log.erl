-module(ra_log).

-include("ra.hrl").

-export([init/2,
         append/3,
         fetch/2,
         take/3,
         last/1
        ]).

-type ra_log_init_args() :: [term()].
-type ra_log_state() :: term().
-type ra_log() :: {Module::atom(), ra_log_state()}.


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


-spec init(Mod::atom(), Args::[term()]) -> ra_log().
init(Mod, Args) ->
    {Mod, Mod:init(Args)}.

-spec fetch(Idx::ra_index(), Log::ra_log()) -> maybe(log_entry()).
fetch(Idx, {Mod, Log}) ->
    Mod:fetch(Idx, Log).

-spec append(Entry::log_entry(), Overwrite::boolean(),
                 State::ra_log()) ->
    {ok, ra_log_state()} | {error, integrity_error}.
append(Entry, Overwrite, {Mod, Log0}) ->
    case Mod:append(Entry, Overwrite, Log0) of
        {ok, Log} ->
            {ok, {Mod, Log}};
        Err ->
            Err
    end.

-spec take(Start::ra_index(), Num::non_neg_integer(),
               State::ra_log()) ->
    [log_entry()].
take(Start, Num, {Mod, Log}) ->
    Mod:take(Start, Num, Log).

-spec last(State::ra_log()) ->
    maybe(log_entry()).
last({Mod, Log}) ->
    Mod:last(Log).
