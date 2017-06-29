-module(ra_lib).

-export([
         ceiling/1,
         default/2,
         dump/1
        ]).

ceiling(X) when X < 0 ->
    trunc(X);
ceiling(X) ->
    T = trunc(X),
    case X-T =:= 0 of
      true -> T;
      false -> T + 1
    end.

default(undefined, Def) ->
    Def;
default(Value, _Def) ->
    Value.

dump(Term) ->
    io:format("Dump: ~p~n", [Term]),
    Term.
