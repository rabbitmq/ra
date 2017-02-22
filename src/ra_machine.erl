-module(ra_machine).


-callback apply(Command::term(), State::term()) ->
    State::term().
