-module(ra_lib).

-export([
         ceiling/1,
         default/2,
         dump/1,
         id/1,
         % cohercion
         to_list/1
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

id(X) -> X.


-spec to_list(atom() | binary() | list()) -> list().
to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.
