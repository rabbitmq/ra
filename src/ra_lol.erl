-module(ra_lol).
%% sorted list of list

-export([
         new/0,
         append/2,
         search/2,
         takewhile/2,
         from_list/1,
         from_list/2,
         to_list/1,
         len/1
        ]).

-define(MAX_ROW_LEN, 64).

-type row() :: [term()].
-type gt_fun() :: fun((Item, Item) -> boolean()).

-record(?MODULE, {len = 0 :: non_neg_integer(),
                  append_row_len = 0 :: non_neg_integer(),
                  gt_fun :: gt_fun(),
                  rows = [] :: [row()]}).

-opaque state() :: #?MODULE{}.

%% a search continuation
-opaque cont() :: [row()].


-export_type([state/0,
              cont/0]).

-spec new() -> state().
new() ->
    #?MODULE{gt_fun = fun erlang:'>'/2}.

-spec new(gt_fun()) -> state().
new(GtFun) ->
    #?MODULE{gt_fun = GtFun}.

%% @doc append an item that is greater than the last appended item
-spec append(Item, state()) ->
    state() | out_of_order
  when Item :: term().
append(Item, #?MODULE{rows = []} = State) ->
    State#?MODULE{rows = [[Item]],
                  len = 1,
                  append_row_len = 0};
append(Item,
       #?MODULE{len = Len,
                gt_fun = GtFun,
                append_row_len = RowLen,
                rows = [[LastItem | _] = Row | Rows]} = State) ->
  case GtFun(Item, LastItem) of
      true ->
          case RowLen of
              ?MAX_ROW_LEN ->
                  %% time for a new row
                  State#?MODULE{rows = [[Item], Row | Rows],
                                len = Len + 1,
                                append_row_len = 1};
              _ ->
                  State#?MODULE{rows = [[Item | Row] | Rows],
                                len = Len + 1,
                                append_row_len = RowLen + 1}
          end;
      false ->
          out_of_order
  end.


-spec search(fun((term()) -> higher | lower | equal),
                 state() | cont()) ->
    {term(), cont()} | undefined.
search(SearchFun, #?MODULE{rows = Rows}) ->
    search(SearchFun, Rows);
search(SearchFun, Rows) when is_list(Rows) ->
    case find_row(SearchFun, Rows) of
        [] ->
            undefined;
        [SearchRow | RemRows] ->
            case search_row(SearchFun, SearchRow) of
                undefined ->
                    undefined;
                {Item, Rem} ->
                    {Item, [Rem | RemRows]}
            end
    end.

-spec takewhile(fun((Item) -> boolean()), state()) ->
    {[Item], state()}
      when Item :: term().
takewhile(Fun, #?MODULE{gt_fun = GtFun} = State) ->
    %% not the most efficient but rarely used
    {Taken, Left} = lists:splitwith(Fun, to_list(State)),
    {Taken, from_list(GtFun, lists:reverse(Left))}.


%% @doc initialise from a list sorted in ascending order
-spec from_list(list()) -> state().
from_list(List) ->
    from_list(fun erlang:'>'/2, List).

-spec from_list(gt_fun(), list()) -> state().
from_list(GtFun, List)
  when is_list(List) ->
    lists:foldl(fun append/2, new(GtFun), List).

-spec to_list(state()) -> list().
to_list(#?MODULE{rows = Rows}) ->
    lists:append(Rows).

-spec len(state()) -> non_neg_integer().
len(#?MODULE{len = Len}) ->
    Len.


%% Internals

search_row(_SearchFun, []) ->
    undefined;
search_row(SearchFun, [Item | Rem]) ->
    case SearchFun(Item) of
        equal ->
            {Item, Rem};
        lower ->
            search_row(SearchFun, Rem);
        higher ->
            undefined
    end.


find_row(SearchFun, [_Row, Row | Rem] = Rows) ->
    %% if last item of the second rows is higher than searching for
    %% then return all rows
    case SearchFun(hd(Row)) of
        higher ->
            Rows;
        _ ->
            %% else keep searching
            find_row(SearchFun, [Row | Rem])
    end;
find_row(_SearchFun, Rows) ->
    Rows.

%%% ===================
%%% Internal unit tests
%%% ===================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

basic_test() ->
    Items = lists:seq(1, 100),
    L0 = ra_lol:from_list(Items),
    ?assertEqual(100, ra_lol:len(L0)),
    ?assertEqual(Items, lists:reverse(ra_lol:to_list(L0))),
    ?assertMatch(out_of_order, ra_lol:append(1, L0)),
    L1 = ra_lol:append(101, L0),
    ?assertEqual(101, ra_lol:len(L1)),
    SearchFun = fun (T) ->
                        fun (Item) ->
                                if T == Item -> equal;
                                   T > Item -> higher;
                                   true -> lower
                                end
                        end
                end,
    [begin
         {T, _} = ra_lol:search(SearchFun(T), L1)
     end || T <- Items ++ [101]],

    %% test searching with a continuation
    _ = lists:foldl(fun (T, Acc) ->
                            {T, Cont} = ra_lol:search(SearchFun(T), Acc),
                            Cont
                    end, L1, lists:reverse(Items ++ [101])),

    TakeFun = fun(Item) -> Item > 50 end,

    {Taken, L2} = takewhile(TakeFun, L1),
    ?assertEqual(50, ra_lol:len(L2)),
    ?assertEqual(51, length(Taken)),
    ?assertMatch(out_of_order, ra_lol:append(50, L2)),
    L3 = ra_lol:append(51, L2),
    ?assertEqual(51, ra_lol:len(L3)),

    ok.


-endif.
