-module(ra_log_memtbl).

-include("ra.hrl").

-export([
         init/1,
         init/2,
         init_successor/3,
         insert/2,
         stage/2,
         commit/1,
         lookup/2,
         lookup_term/2,
         tid_for/3,
         fold/5,
         get_items/2,
         record_flushed/3,
         set_first/2,
         set_last/2,
         delete/2,
         tid/1,
         prev/1,
         info/1,
         range/1,
         delete/1
        ]).

% -record(cfg, {}).
%

-define(IN_RANGE(Idx, Range),
        (Idx >= element(1, Range) andalso
         Idx =< element(2, Range))).

-define(IS_BEFORE_RANGE(Idx, Range),
        (is_tuple(Range) andalso
         Idx < element(1, Range))).

-define(IS_AFTER_RANGE(Idx, Range),
        (is_tuple(Range) andalso
         Idx > element(2, Range))).

% -define(RANGE_OUT(Idx, Range),
%         (Idx < element(1, Range) orelse
%          Idx > element(2, Range))).

-define(IS_NEXT_IDX(Idx, Range),
        (Range == undefined orelse
         Idx == element(2, Range) + 1)).

-record(?MODULE,
        {tid :: ets:tid(),
         range :: undefined | {ra:index(), ra:index()},
         staged :: undefined | {NumStaged :: non_neg_integer(), [log_entry()]},
         prev :: undefined | #?MODULE{}
        }).

-opaque state() :: #?MODULE{}.

-type delete_spec() :: ra:index() |
                       {'<', ra:index()} |
                       {all, ets:tid()} |
                       {delete, ets:tid()} |
                       {range, ets:tid(), ra:range()}.
-export_type([
              state/0,
              delete_spec/0
              ]).

-spec init(ets:tid(), read | read_write) -> state().
init(Tid, Mode) ->
    Range = case Mode of
                read ->
                    undefined;
                read_write ->
                    %% TODO: this is potentially horrendously slow
                    case ets:tab2list(Tid) of
                        [] ->
                            undefined;
                        Entries ->
                            lists:foldl(
                              fun ({I, _, _}, undefined) ->
                                      {I, I};
                                  ({I, _, _}, {S, E}) ->
                                      {min(I, S), max(I, E)}
                              end, undefined, Entries)
                    end
            end,

    #?MODULE{tid = Tid,
             range = Range
            }.

-spec init(ets:tid()) -> state().
init(Tid) ->
    init(Tid, read_write).

-spec init_successor(ets:tid(), read | read_write, state()) -> state().
init_successor(Tid, Mode, #?MODULE{} = State) ->
    %%TODO: mt: I suspec the mode would always be read_write
    Succ = init(Tid, Mode),
    Succ#?MODULE{prev = State}.

-spec insert(log_entry(), state()) ->
    {ok, state()} | {error, overwriting}.
insert({Idx, _, _} = Entry,
       #?MODULE{tid = Tid,
                range = Range} = State)
  when ?IS_NEXT_IDX(Idx, Range) ->
    true = ets:insert(Tid, Entry),
    {ok, State#?MODULE{range = update_range_end(Idx, Range)}};
insert({Idx, _, _} = _Entry,
       #?MODULE{range = Range} = _State0)
  when ?IN_RANGE(Idx, Range) orelse
       ?IS_BEFORE_RANGE(Idx, Range) ->
    {error, overwriting}.

-spec stage(log_entry(), state()) -> state().
stage({Idx, _, _} = Entry,
       #?MODULE{ staged = {FstIdx, Staged},
                range = Range} = State)
  when ?IS_NEXT_IDX(Idx, Range) ->
    State#?MODULE{staged = {FstIdx, [Entry | Staged]},
                  range = update_range_end(Idx, Range)};
stage({Idx, _, _} = Entry,
       #?MODULE{tid = _Tid,
                staged = undefined,
                range = Range} = State)
  when ?IS_NEXT_IDX(Idx, Range) ->
    State#?MODULE{staged = {Idx, [Entry]},
                  range = update_range_end(Idx, Range)};
stage({Idx, _, _} = _Entry,
       #?MODULE{range = Range} = _State0)
  when ?IN_RANGE(Idx, Range) orelse
       ?IS_BEFORE_RANGE(Idx, Range) ->
    exit({error, overwriting}).

-spec commit(state()) -> state().
commit(#?MODULE{staged = undefined} = State) ->
    State;
commit(#?MODULE{tid = Tid,
                staged = {_, Staged0}} = State) ->
    Staged = lists:reverse(Staged0),
    true = ets:insert(Tid, Staged),
    {Staged, State#?MODULE{staged = undefined}}.

-spec lookup(ra:index(), state()) ->
    log_entry() | undefined.
lookup(Idx, #?MODULE{staged = {FstStagedIdx, Staged}})
  when Idx >= FstStagedIdx ->
    %% staged read
    case lists:keysearch(Idx, 1, Staged) of
        {value, Entry} ->
            Entry;
        _ ->
            undefined
    end;
lookup(Idx, #?MODULE{tid = Tid,
                     staged = undefined}) ->
    case ets:lookup(Tid, Idx) of
        [Entry] ->
            Entry;
        _ ->
            undefined
    end.

-spec lookup_term(ra:index(), state()) ->
    ra_term() | undefined.
lookup_term(Idx, #?MODULE{staged = {FstStagedIdx, Staged}})
  when Idx >= FstStagedIdx ->
    %% staged read
    case lists:keysearch(Idx, 1, Staged) of
        {value, {_, T, _}} ->
            T;
        _ ->
            undefined
    end;
lookup_term(Idx, #?MODULE{tid = Tid,
                          range = Range})
  when ?IN_RANGE(Idx, Range) ->
    ets:lookup_element(Tid, Idx, 2);
lookup_term(_Idx, _State) ->
    undefined.

-spec tid_for(ra:index(), ra_term(), state()) ->
    undefine | ets:tid().
tid_for(_Idx, _Term, undefined) ->
    undefined;
tid_for(Idx, Term, State) ->
    case lookup_term(Idx, State) of
        Term ->
            tid(State);
        _ ->
            tid_for(Idx, Term, State#?MODULE.prev)
    end.

-spec fold(ra:index(), ra:index(), fun(), term(), state()) ->
    term().
fold(To, To, Fun, Acc, State) ->
    E = lookup(To, State),
    Fun(E, Acc);
fold(From, To, Fun, Acc, State)
  when To > From ->
    E = lookup(From, State),
    fold(From + 1, To, Fun, Fun(E, Acc), State).

-spec get_items([ra:index()], state()) ->
    {[log_entry()],
     NumRead :: non_neg_integer(),
     Remaining :: [ra:index()]}.
get_items(Indexes, #?MODULE{} = State) ->
    read_sparse(Indexes, State, []).

-spec delete(delete_spec(), state()) ->
    non_neg_integer().
delete(undefined, #?MODULE{}) ->
    0;
delete(Idx, #?MODULE{tid = Tid}) when is_integer(Idx) ->
    %% TODO: this is designed to be called from another process so we
    %% cant rely on range, may need to revise API during optimisation
    true = ets:delete(Tid, Idx),
    1;
delete({range, Tid, {Start, End}}, #?MODULE{tid = Tid} = State) ->
    NumToDelete = End - Start + 1,
    Limit = ets:info(Tid, size) div 2,
    case NumToDelete > Limit of
        true ->
            %% more than half the table is to be deleted
            delete({'<', Tid, End + 1}, State);
        false ->
            delete(Start, End, Tid),
            End - Start + 1
    end;
delete({Op, Tid, Idx}, #?MODULE{tid = Tid})
  when is_integer(Idx) and is_atom(Op) ->
    DelSpec = [{{'$1', '_', '_'}, [{'<', '$1', Idx}], [true]}],
    ets:select_delete(Tid, DelSpec);
delete({all, Tid}, #?MODULE{tid = Tid}) ->
    Size = ets:info(Tid, size),
    true = ets:delete_all_objects(Tid),
    Size.

-spec range(state()) ->
    undefined | {ra:index(), ra:index()}.
range(#?MODULE{range = Range,
               prev = undefined}) ->
    Range;
range(#?MODULE{range = Range,
               prev = Prev}) ->
    PrevRange = ra_log_memtbl:range(Prev),
    ra_range:add(PrevRange, Range).

-spec delete(state()) -> ok.
delete(#?MODULE{tid = Tid}) ->
    _ = ets:delete(Tid),
    ok.

-spec tid(state()) -> ets:tid().
tid(#?MODULE{tid = Tid}) ->
    Tid.

-spec prev(state()) -> undefined | state().
prev(#?MODULE{prev = Prev}) ->
    Prev.

-spec info(state()) -> map().
info(#?MODULE{tid = Tid,
              prev = Prev} = State) ->
    #{tid => Tid,
      size => ets:info(Tid, size),
      name => ets:info(Tid, name),
      range => range(State),
      has_previous => Prev =/= undefined
     }.

-spec record_flushed(ets:tid(), ra:range(), state()) -> {delete_spec(), state()}.
record_flushed(Tid, {_, End}, #?MODULE{tid = Tid} = State0) ->
    case set_first(End+1, State0) of
        {[], State} ->
            {undefined, State};
        {[Spec], State} ->
            {Spec, State}
    end;
record_flushed(_Tid, _Range, #?MODULE{prev = undefined} = State) ->
    {undefined, State};
record_flushed(Tid, Range, #?MODULE{prev = Prev0} = State) ->
    case record_flushed(Tid, Range, Prev0) of
        {{all, Tid}, _Prev} ->
            %% upgrade all spec to delete
            {{delete, Tid}, State#?MODULE{prev = undefined}};
        {Spec, Prev} ->
            {Spec, State#?MODULE{prev = Prev}}
    end.

-spec set_first(ra:index(), state()) ->
    {[delete_spec()], state()}.
set_first(Idx, #?MODULE{tid = Tid,
                        range = {Start, _} = Range} = State)
  when ?IN_RANGE(Idx, Range) ->
    {[{range, Tid, {Start, Idx - 1}}],
     State#?MODULE{range = update_range_start(Idx, Range)}};
set_first(Idx, #?MODULE{tid = Tid,
                        range = Range} = State)
  when ?IS_AFTER_RANGE(Idx, Range) ->
    {[{all, Tid}], State#?MODULE{range = undefined}};
set_first(_Idx, State) ->
    %% TODO fall back to prev
    {[], State}.

-spec set_last(ra:index(), state()) ->
    {undefined |  delete_spec(), state()}.
set_last(Idx, #?MODULE{range = Range} = State)
  when ?IN_RANGE(Idx, Range) ->
    {{'>', Idx}, State#?MODULE{range = update_range_end(Idx, Range)}};
set_last(Idx, #?MODULE{range = {Start, _End}} = State)
  when Idx < Start ->
    {all, State#?MODULE{range = undefined}};
set_last(_Idx, State) ->
    {undefined, State}.

%% internal

update_range_end(Idx, {Start, End})
  when Idx =< End orelse
       Idx == End + 1 ->
    {Start, Idx};
update_range_end(Idx, undefined) ->
    {Idx, Idx}.

update_range_start(Idx, {Start, End})
  when Idx >= Start andalso
       Idx =< End ->
    {Idx, End};
update_range_start(_Idx, Range) ->
    Range.

delete(End, End, Tid) ->
    ets:delete(Tid, End);
delete(Start, End, Tid) ->
    _ = ets:delete(Tid, Start),
    delete(Start+1, End, Tid).

read_sparse(Indexes, State, Acc) ->
    read_sparse(Indexes, State, 0, Acc).

read_sparse([], _State, Num, Acc) ->
    {Acc, Num, []}; %% no remainder
read_sparse([Next | Rem] = Indexes, State, Num, Acc) ->
    case lookup(Next, State) of
        undefined ->
            {Acc, Num, Indexes};
        Entry ->
            read_sparse(Rem, State, Num + 1, [Entry | Acc])
    end.

