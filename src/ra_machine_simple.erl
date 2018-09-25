-module(ra_machine_simple).
-behaviour(ra_machine).

-export([
         init/1,
         apply/4
         ]).

init(#{simple_fun := Fun,
       initial_state := Initial}) ->
    {{simple, Fun, Initial}, []}.

apply(_, Cmd, Effects, {simple, Fun, State}) ->
    Next = Fun(Cmd, State),
    %% return the next state as the reply as well
    {{simple, Fun, Next}, Effects, Next}.


