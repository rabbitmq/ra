-module(ra_log_memtbl).

-include("ra.hrl").

-export([
         init/1,
         insert/2,
         stage/2,
         commit/1,
         lookup/2,
         lookup_term/2,
         fold/5,
         get_items/2,
         set_first/2,
         set_last/2,
         delete/2,
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
        {tbl :: ets:tid(),
         range :: undefined | {ra:index(), ra:index()},
         staged :: undefined | {NumStaged :: non_neg_integer(), [log_entry()]}
        }).

-opaque state() :: #?MODULE{}.

-type delete_spec() :: ra:index() | {'<', ra:index()}.
-export_type([
              state/0
              ]).

-spec init(ets:tid()) ->
    {ok, state()} | {error, term()}.
init(Tid) ->
    %% TODO: mt: optimise
    Range = case ets:tab2list(Tid) of
                [] ->
                    undefined;
                Entries ->
                    lists:foldl(
                      fun ({I, _, _}, undefined) ->
                              {I, I};
                          ({I, _, _}, {S, E}) ->
                              {min(I, S), max(I, E)}
                      end, undefined, Entries)
            end,
    #?MODULE{tbl = Tid,
             range = Range
             % cache = #{}
            }.

-spec insert(log_entry(), state()) -> state().
insert({Idx, _, _} = Entry,
       #?MODULE{tbl = Tid,
                range = Range} = State)
  when ?IS_NEXT_IDX(Idx, Range) ->
    % ct:pal("mem tbl insert ~p, ~p", [Entry, Range]),
    true = ets:insert(Tid, Entry),
    State#?MODULE{range = update_range_end(Idx, Range)};
insert({Idx, _, _} = _Entry,
       #?MODULE{range = Range} = _State0)
  when ?IN_RANGE(Idx, Range) orelse
       ?IS_BEFORE_RANGE(Idx, Range) ->
    exit({error, overwriting}).
    % ct:pal("mem tbl insert out ~p, ~p", [Idx, Range]),
    %% TODO: we may need to return {error, overwriting} and have the caller
    %% call set_last/2 explicitly
    % {Spec, State} = set_last(Idx - 1, State0),
    % _ = delete(Spec, State),
    % insert(Entry, State).

-spec stage(log_entry(), state()) -> state().
stage({Idx, _, _} = Entry,
       #?MODULE{ staged = {FstIdx, Staged},
                range = Range} = State)
  when ?IS_NEXT_IDX(Idx, Range) ->
    State#?MODULE{staged = {FstIdx, [Entry | Staged]},
                  range = update_range_end(Idx, Range)};
stage({Idx, _, _} = Entry,
       #?MODULE{tbl = _Tid,
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
commit(#?MODULE{tbl = Tid,
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
lookup(Idx, #?MODULE{tbl = Tid,
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
lookup_term(Idx, #?MODULE{tbl = Tid,
                          range = Range})
  when ?IN_RANGE(Idx, Range) ->
    ets:lookup_element(Tid, Idx, 2);
lookup_term(_Idx, _State) ->
    undefined.

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
delete(Idx, #?MODULE{tbl = Tid}) when is_integer(Idx) ->
    %% TODO: this is designed to be called from another process so we
    %% cant rely on range, may need to revise API during optimisation
    true = ets:delete(Tid, Idx),
    1;
delete({range, {Start, End}}, #?MODULE{tbl = Tid}) ->
    delete(Start, End, Tid),
    End - Start + 1;
delete({Op, Idx}, #?MODULE{tbl = Tid})
  when is_integer(Idx) and is_atom(Op) ->
    DelSpec = [{{'$1', '_', '_'}, [{'<', '$1', Idx}], [true]}],
    ets:select_delete(Tid, DelSpec);
delete(all, #?MODULE{tbl = Tid}) ->
    Size = ets:info(Tid, size),
    true = ets:delete_all_objects(Tid),
    Size.

-spec range(state()) ->
    undefined | {ra:index(), ra:index()}.
range(#?MODULE{range = Range}) ->
    Range.

-spec delete(state()) -> ok.
delete(#?MODULE{tbl = Tid}) ->
    _ = ets:delete(Tid),
    ok.

-spec info(state()) -> map().
info(#?MODULE{tbl = Tid}) ->
    #{tid => Tid,
      size => ets:info(Tid, size),
      name => ets:info(Tid, name)
     }.

-spec set_first(ra:index(), state()) ->
    {undefined |  delete_spec(), state()}.
set_first(Idx, #?MODULE{range = {_Start, _} = Range} = State)
  when ?IN_RANGE(Idx, Range) ->
    {{'<', Idx}, State#?MODULE{range = update_range_start(Idx, Range)}};
    % {{range, {Start, Idx - 1}},
    %  State#?MODULE{range = update_range_start(Idx, Range)}};
set_first(Idx, #?MODULE{range = Range} = State)
  when ?IS_AFTER_RANGE(Idx, Range) ->
    {all, State#?MODULE{range = undefined}};
set_first(_Idx, State) ->
    {undefined, State}.

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

