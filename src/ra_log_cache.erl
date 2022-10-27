-module(ra_log_cache).

-include("ra.hrl").

-export([
         init/0,
         reset/1,
         add/3,
         fetch/2,
         fetch/3,
         get_items/2,
         get_items/3,
         trim/3,
         flush/1,
         needs_flush/1,
         size/1
         ]).

%% holds static or rarely changing fields
% -record(cfg, {}).

-record(?MODULE, {tbl :: ets:tid(),
                  % range :: undefined | {ra:index(), ra:index()},
                  cache = #{} :: #{ra:index() => log_entry()}}).

-opaque state() :: #?MODULE{}.

-export_type([
              state/0
              ]).

-spec init() -> state().
init() ->
    Tid = ets:new(?MODULE, [set, private]),
    #?MODULE{tbl = Tid}.

-spec reset(state()) -> state().
reset(#?MODULE{tbl = Tid} = State) ->
    true = ets:delete_all_objects(Tid),
    State#?MODULE{cache = #{}}.

-spec add(ra:index(), log_entry(), state()) -> state().
add(Idx, Entry, #?MODULE{cache = Cache} = State) ->
    State#?MODULE{cache = maps:put(Idx, Entry, Cache)}.

-spec fetch(ra:index(), state()) -> log_entry().
fetch(Idx, State) ->
    case fetch(Idx, State, undefined) of
        undefined ->
            exit({ra_log_cache_key_not_found, Idx});
        Item ->
            Item
    end.

-spec fetch(ra:index(), state(), term()) -> term() | log_entry().
fetch(Idx, #?MODULE{tbl = Tid, cache = Cache}, Default) ->
    case maps:get(Idx, Cache, undefined) of
        undefined ->
            case ets:lookup(Tid, Idx) of
                [] ->
                    Default;
            [Item] ->
                    Item
            end;
        Item ->
            Item
    end.

-spec get_items(From :: ra:index(), To :: ra:index(), state()) ->
    [log_entry()].
get_items(From, To, #?MODULE{tbl = Tid, cache = Cache}) ->
    get_cache_items(From, To, Cache, Tid, []).

-spec get_items([ra:index()], state()) ->
    {[log_entry()],
     NumRead :: non_neg_integer(),
     Remaining :: [ra:index()]}.
get_items(Indexes, #?MODULE{tbl = Tid, cache = Cache}) ->
    cache_read_sparse(Indexes, Cache, Tid, []).

-spec trim(ra:index(), ra:index(), state()) -> state().
trim(From, To, #?MODULE{} = State)
  when From > To ->
    State;
trim(From, To, #?MODULE{tbl = Tid, cache = Cache} = State) ->
    State#?MODULE{cache = cache_without(From, To, Cache, Tid)}.

-spec flush(state()) -> state().
flush(#?MODULE{tbl = Tid,
               cache = Cache} = State)
  when map_size(Cache) > 0 ->
    _ = ets:insert(Tid, maps:values(Cache)),
    State#?MODULE{cache = #{}};
flush(State) ->
    State.

-spec needs_flush(state()) -> boolean().
needs_flush(#?MODULE{cache = Cache}) ->
    map_size(Cache) > 0.

-spec size(state()) -> non_neg_integer().
size(#?MODULE{tbl = Tid, cache = Cache}) ->
    map_size(Cache) + ets:info(Tid, size).

%% INTERNAL
%%

cache_without(Idx, Idx, Cache, Tid) ->
    _ = ets:delete(Tid, Idx),
    maps:remove(Idx, Cache);
cache_without(FromIdx, ToIdx, Cache, Tid)
  when is_map_key(FromIdx, Cache) ->
    cache_without(FromIdx + 1, ToIdx, maps:remove(FromIdx, Cache), Tid);
cache_without(FromIdx, ToIdx, Cache, Tid) ->
    _ = ets:delete(Tid, FromIdx),
    cache_without(FromIdx + 1, ToIdx, Cache, Tid).

get_cache_items(From, To, _Cache, _Tid, Acc)
  when From > To ->
    Acc;
get_cache_items(From, To, Cache, Tid, Acc) ->
    case Cache of
        #{To := Entry} ->
            get_cache_items(From, To - 1, Cache, Tid, [Entry | Acc]);
        _ ->
            case ets:lookup(Tid, To) of
                [] ->
                    Acc;
                [Entry] ->
                    get_cache_items(From, To - 1, Cache, Tid, [Entry | Acc])
            end
    end.

cache_read_sparse(Indexes, Cache, Tid, Acc) ->
    cache_read_sparse(Indexes, Cache, Tid, 0, Acc).

cache_read_sparse([], _Cache, _Tid, Num, Acc) ->
    {Acc, Num, []}; %% no reminder
cache_read_sparse([Next | Rem] = Indexes, Cache, Tid, Num, Acc) ->
    case Cache of
        #{Next := Entry} ->
            cache_read_sparse(Rem, Cache, Tid, Num + 1, [Entry | Acc]);
        _ ->
            case ets:lookup(Tid, Next) of
                [] ->
                    {Acc, Num, Indexes};
                [Entry] ->
                    cache_read_sparse(Rem, Cache, Tid, Num + 1, [Entry | Acc])
            end
    end.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
