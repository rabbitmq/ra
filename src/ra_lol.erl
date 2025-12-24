-module(ra_lol).
%% Adaptive sorted list structure that uses:
%% - Simple list for small collections (< 65 elements) - fast pattern matching
%% - Tuple with binary search for larger collections (>= 65 elements)
%%
%% This provides optimal performance across all sizes.

-export([
         new/0,
         new/1,
         append/2,
         search/2,
         takewhile/2,
         from_list/1,
         from_list/2,
         to_list/1,
         len/1
        ]).

%% Threshold at or above which we use tuple, below we use simple list
-define(TUPLE_THRESHOLD, 65).

-type gt_fun() :: fun((Item, Item) -> boolean()).

%% State is either:
%% - {list, GtFun, List} where List is stored in descending order (newest first)
%% - {tuple, GtFun, Len, Data} where Data is a tuple in descending order
-opaque state() :: {list, gt_fun(), list()} 
                 | {tuple, gt_fun(), non_neg_integer(), tuple()}.

%% Continuation is either:
%% - A simple list (remaining elements to search)
%% - Tuple continuation {Data, Pos, High}
-opaque cont() :: list() | {tuple(), pos_integer(), non_neg_integer()}.

-export_type([state/0,
              cont/0]).

-spec new() -> state().
new() ->
    {list, fun erlang:'>'/2, []}.

-spec new(gt_fun()) -> state().
new(GtFun) ->
    {list, GtFun, []}.

%% @doc append an item that is greater than the last appended item
-spec append(Item, state()) ->
    state() | out_of_order
  when Item :: term().
append(Item, {list, GtFun, []}) ->
    {list, GtFun, [Item]};
append(Item, {list, GtFun, [Last | _] = List}) ->
    case GtFun(Item, Last) of
        true ->
            NewList = [Item | List],
            maybe_upgrade(GtFun, NewList);
        false ->
            out_of_order
    end;
append(Item, {tuple, GtFun, Len, Data}) ->
    %% Last appended item is at index 1 (newest first)
    LastItem = element(1, Data),
    case GtFun(Item, LastItem) of
        true ->
            %% Prepend by converting to list and back - O(N)
            NewData = list_to_tuple([Item | tuple_to_list(Data)]),
            {tuple, GtFun, Len + 1, NewData};
        false ->
            out_of_order
    end.

-spec search(fun((term()) -> higher | lower | equal),
             state() | cont()) ->
    {term(), cont()} | undefined.
search(_SearchFun, {list, _GtFun, []}) ->
    undefined;
search(SearchFun, {list, _GtFun, List}) ->
    list_search(SearchFun, List);
search(_SearchFun, {tuple, _GtFun, 0, _Data}) ->
    undefined;
search(SearchFun, {tuple, _GtFun, Len, Data}) ->
    %% Use binary search for tuple
    binary_search(SearchFun, Data, 1, Len);
search(SearchFun, Cont) when is_list(Cont) ->
    %% List continuation - continue searching the remaining list
    list_search(SearchFun, Cont);
search(SearchFun, {Data, Pos, High}) when is_tuple(Data), Pos =< High ->
    %% Tuple continuation - use linear scan for sequential access
    tuple_linear_search(SearchFun, Data, Pos, High);
search(_SearchFun, {Data, _Pos, _High}) when is_tuple(Data) ->
    undefined.

%% Simple list search with fast pattern matching
list_search(_SearchFun, []) ->
    undefined;
list_search(SearchFun, [Item | Rest]) ->
    case SearchFun(Item) of
        equal ->
            %% Found! Return item and remaining list as continuation
            {Item, Rest};
        lower ->
            %% Keep searching (toward smaller/older items)
            list_search(SearchFun, Rest);
        higher ->
            %% We've gone past where the item would be
            undefined
    end.

%% Binary search for tuple - O(log N)
%% Data is sorted descending (index 1 = largest/newest)
binary_search(_SearchFun, _Data, Low, High) when Low > High ->
    undefined;
binary_search(SearchFun, Data, Low, High) ->
    Mid = (Low + High) div 2,
    Item = element(Mid, Data),
    case SearchFun(Item) of
        equal ->
            %% Found! Continuation points to next element
            {Item, {Data, Mid + 1, tuple_size(Data)}};
        higher ->
            %% Target is "higher" - in descending order, at lower indices
            binary_search(SearchFun, Data, Low, Mid - 1);
        lower ->
            %% Target is "lower" - in descending order, at higher indices
            binary_search(SearchFun, Data, Mid + 1, High)
    end.

%% Linear search on tuple for continuations - O(1) amortized for sequential access
tuple_linear_search(SearchFun, Data, Pos, High) when Pos =< High ->
    Item = element(Pos, Data),
    case SearchFun(Item) of
        equal ->
            {Item, {Data, Pos + 1, High}};
        lower ->
            tuple_linear_search(SearchFun, Data, Pos + 1, High);
        higher ->
            undefined
    end;
tuple_linear_search(_SearchFun, _Data, _Pos, _High) ->
    undefined.

-spec takewhile(fun((Item) -> boolean()), state()) ->
    {[Item], state()}
      when Item :: term().
takewhile(Fun, {list, GtFun, List}) ->
    {Taken, Left} = lists:splitwith(Fun, List),
    {Taken, {list, GtFun, Left}};
takewhile(Fun, {tuple, GtFun, _Len, Data}) ->
    List = tuple_to_list(Data),
    {Taken, Left} = lists:splitwith(Fun, List),
    %% Rebuild appropriate structure based on remaining size
    LeftLen = length(Left),
    NewState = if LeftLen >= ?TUPLE_THRESHOLD ->
                      {tuple, GtFun, LeftLen, list_to_tuple(Left)};
                  true ->
                      {list, GtFun, Left}
               end,
    {Taken, NewState}.

%% @doc initialise from a list sorted in ascending order
-spec from_list(list()) -> state().
from_list(List) ->
    from_list(fun erlang:'>'/2, List).

-spec from_list(gt_fun(), list()) -> state().
from_list(GtFun, List) when is_list(List) ->
    Len = length(List),
    if Len >= ?TUPLE_THRESHOLD ->
           %% Store in descending order (newest/largest first) as tuple
           {tuple, GtFun, Len, list_to_tuple(lists:reverse(List))};
       true ->
           %% Store in descending order as list
           {list, GtFun, lists:reverse(List)}
    end.

-spec to_list(state()) -> list().
to_list({list, _GtFun, List}) ->
    List;
to_list({tuple, _GtFun, _Len, Data}) ->
    tuple_to_list(Data).

-spec len(state()) -> non_neg_integer().
len({list, _GtFun, List}) ->
    length(List);
len({tuple, _GtFun, Len, _Data}) ->
    Len.

%%% ===================
%%% Internal functions
%%% ===================

%% Upgrade from list to tuple if we've crossed the threshold
maybe_upgrade(GtFun, List) ->
    Len = length(List),
    if Len >= ?TUPLE_THRESHOLD ->
           %% Convert to tuple
           {tuple, GtFun, Len, list_to_tuple(List)};
       true ->
           {list, GtFun, List}
    end.

%%% ===================
%%% Internal unit tests
%%% ===================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
    Items = lists:seq(1, 100),
    L0 = ?MODULE:from_list(Items),
    ?assertEqual(100, ?MODULE:len(L0)),
    ?assertEqual(Items, lists:reverse(?MODULE:to_list(L0))),
    ?assertMatch(out_of_order, ?MODULE:append(1, L0)),
    L1 = ?MODULE:append(101, L0),
    ?assertEqual(101, ?MODULE:len(L1)),
    SearchFun = fun (T) ->
                        fun (Item) ->
                                if T == Item -> equal;
                                   T > Item -> higher;
                                   true -> lower
                                end
                        end
                end,
    [begin
         {T, _} = ?MODULE:search(SearchFun(T), L1)
     end || T <- Items ++ [101]],

    %% test searching with a continuation
    _ = lists:foldl(fun (T, Acc) ->
                            {T, Cont} = ?MODULE:search(SearchFun(T), Acc),
                            Cont
                    end, L1, lists:reverse(Items ++ [101])),

    TakeFun = fun(Item) -> Item > 50 end,

    {Taken, L2} = takewhile(TakeFun, L1),
    ?assertEqual(50, ?MODULE:len(L2)),
    ?assertEqual(51, length(Taken)),
    ?assertMatch(out_of_order, ?MODULE:append(50, L2)),
    L3 = ?MODULE:append(51, L2),
    ?assertEqual(51, ?MODULE:len(L3)),

    ok.

%% Test that small lists use simple list
small_uses_list_test() ->
    Items = lists:seq(1, 20),
    {list, _, _} = ?MODULE:from_list(Items).

%% Test that large lists use tuple
large_uses_tuple_test() ->
    Items = lists:seq(1, 100),
    {tuple, _, _, _} = ?MODULE:from_list(Items).

%% Test upgrade from list to tuple via append
upgrade_test() ->
    Items = lists:seq(1, 64),
    L0 = ?MODULE:from_list(Items),
    {list, _, _} = L0,
    L1 = ?MODULE:append(65, L0),
    {tuple, _, _, _} = L1,
    ?assertEqual(65, ?MODULE:len(L1)).

-endif.
