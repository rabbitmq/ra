-module(ra_lru).

-export([
         new/1,
         put/3,
         get/2
        ]).

%%% Non-strict pure data-structure LRU cache suitable for smaller caches
%%% Actual size may be twice the size requested. Evictions typically
%%% happens in batches.

-type ra_lru() :: {Size :: non_neg_integer(), New :: map() | undefined,
                   Old :: map()}.

-spec new(Size :: non_neg_integer()) -> ra_lru().
new(Size) ->
    {Size, #{}, #{}}.

-spec put(Key :: term(), Value :: term(), Cache :: ra_lru()) ->
    undefined | {Cache :: ra_lru(), Evicted :: undefined | #{term() => term()}}.
put(Key, Value, {Size, New0, Old0}) ->
    New = maps:put(Key, Value, New0),
    case maps:size(New) of
        Size ->
            % key could be in old - need to remove it from evictions!
            {{Size, #{}, New}, maps:remove(Key, Old0)};
        _ ->
            {{Size, New, Old0}, #{}}
    end.

-spec get(Key :: term(), Cache :: ra_lru()) ->
    undefined | {Value :: term(), Cache :: ra_lru(),
                 Evicted :: #{term => term()}}.
get(Key, {Size, New0, Old0} = Cache) ->
    case New0 of
        #{Key := Value} ->
            {Value, Cache};
        _ ->
            case {Old0, maps:size(New0)} of
                {#{Key := Value}, Size} ->
                    % move New to Old and evict all old
                    New = maps:put(Key, Value, #{}),
                    {Value, {Size, New, New0}, maps:remove(Key, Old0)};
                {#{Key := Value}, _} ->
                    % move from old to new
                    New = maps:put(Key, Value, New0),
                    {Value, {Size, New, maps:remove(Key, Old0)}};
                _ ->
                    undefined
            end
    end.



-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basics_test() ->
    C0 = new(3),
    undefined = get(a, C0),
    {C, _} = put(a, "a", C0),
    {"a", _} = get(a, C),
    {C1, _} = put(b, "b", C),
    {C2, _} = put(c, "c", C1),
    {C3, _} = put(d, "d", C2),
    {C4, _} = put(e, "e", C3),
    {C5, _} = put(f, "f", C4),
    undefined = get(a, C5),
    ok.

-endif.
