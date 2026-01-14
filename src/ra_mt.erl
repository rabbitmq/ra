%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term Broadcom refers to Broadcom Inc. and/or its subsidiaries.
%% @hidden
-module(ra_mt).

-include("ra.hrl").

-export([
         init/1,
         init/2,
         init_successor/3,
         insert/2,
         stage/2,
         commit/1,
         abort/1,
         lookup/2,
         lookup_term/2,
         tid_for/3,
         fold/5,
         get_items/2,
         record_flushed/3,
         set_first/2,
         delete/1,
         tid/1,
         staged/1,
         is_active/2,
         prev/1,
         info/1,
         range/1,
         range_overlap/2
        ]).

-define(MAX_MEMTBL_ENTRIES, 1_000_000).

-define(IN_RANGE(Idx, Range),
        (is_tuple(Range) andalso
         Idx >= element(1, Range) andalso
         Idx =< element(2, Range))).

-define(IS_BEFORE_RANGE(Idx, Range),
        (is_tuple(Range) andalso
         Idx < element(1, Range))).

% -define(IS_AFTER_RANGE(Idx, Range),
%         (is_tuple(Range) andalso
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

-type delete_spec() :: undefined |
                       {'<', ets:tid(), ra:index()} |
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
                    %% TODO: can this be optimised further?
                    ets:foldl(fun ({I, _, _}, undefined) ->
                                      {I, I};
                                  ({I, _, _}, {S, E}) ->
                                      {min(I, S), max(I, E)}
                              end, undefined, Tid)
            end,
    #?MODULE{tid = Tid,
             range = Range
            }.

-spec init(ets:tid()) -> state().
init(Tid) ->
    init(Tid, read_write).

-spec init_successor(ets:tid(), read | read_write, state()) -> state().
init_successor(Tid, Mode, #?MODULE{} = State) ->
    Succ = init(Tid, Mode),
    Succ#?MODULE{prev = State}.

-spec insert(log_entry(), state()) ->
    {ok, state()} | {error, overwriting | limit_reached}.
insert({Idx, _, _} = Entry,
       #?MODULE{tid = Tid,
                range = Range} = State)
  when ?IS_NEXT_IDX(Idx, Range) ->
    case ra_range:size(Range) > ?MAX_MEMTBL_ENTRIES of
        true ->
            {error, limit_reached};
        false ->
            true = ets:insert(Tid, Entry),
            {ok, State#?MODULE{range = update_range_end(Idx, Range)}}
    end;
insert({Idx, _, _} = _Entry,
       #?MODULE{range = Range} = _State0)
  when ?IN_RANGE(Idx, Range) orelse
       ?IS_BEFORE_RANGE(Idx, Range) ->
    {error, overwriting}.

-spec stage(log_entry(), state()) ->
    {ok, state()} | {error, overwriting | limit_reached}.
stage({Idx, _, _} = Entry,
      #?MODULE{staged = {FstIdx, Staged},
               range = Range} = State)
  when ?IS_NEXT_IDX(Idx, Range) ->
    {ok, State#?MODULE{staged = {FstIdx, [Entry | Staged]},
                       range = update_range_end(Idx, Range)}};
stage({Idx, _, _} = Entry,
      #?MODULE{tid = _Tid,
               staged = undefined,
               range = Range} = State)
  when ?IS_NEXT_IDX(Idx, Range) ->
    case ra_range:size(Range) > ?MAX_MEMTBL_ENTRIES of
        true ->
            %% the limit cannot be reached during transaction
            {error, limit_reached};
        false ->
            {ok, State#?MODULE{staged = {Idx, [Entry]},
                               range = update_range_end(Idx, Range)}}
    end;
stage({Idx, _, _} = _Entry,
      #?MODULE{range = Range} = _State0)
  when ?IN_RANGE(Idx, Range) orelse
       ?IS_BEFORE_RANGE(Idx, Range) ->
    {error, overwriting}.

-spec commit(state()) -> {[log_entry()], state()}.
commit(#?MODULE{staged = undefined} = State) ->
    {[], State};
commit(#?MODULE{tid = Tid,
                staged = {_, Staged0},
                prev = Prev0} = State) ->
    {PrevStaged, Prev} = case Prev0 of
                             undefined ->
                                 {[], Prev0};
                             _ ->
                                 commit(Prev0)
                         end,
    Staged = lists:reverse(Staged0),
    case catch ets:insert(Tid, Staged) of
        true ->
            %% TODO: mt: could prev contain overwritten entries?
            {PrevStaged ++ Staged, State#?MODULE{staged = undefined,
                                                 prev = Prev}};
        {'EXIT', {badarg, _}} ->
            {[], State#?MODULE{staged = undefined,
                               prev = Prev}}
    end.

-spec abort(state()) -> state().
abort(#?MODULE{staged = undefined} = State) ->
    State;
abort(#?MODULE{staged = {_, Staged},
               range = Range,
               prev = Prev0} = State) ->
    Prev = case Prev0 of
               undefined ->
                   Prev0;
               _ ->
                   abort(Prev0)
           end,
    {Idx, _, _} = lists:last(Staged),
    State#?MODULE{staged = undefined,
                  range = ra_range:limit(Idx, Range),
                  prev = Prev}.

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
                     range = Range,
                     prev = Prev,
                     staged = undefined}) ->
    case ?IN_RANGE(Idx, Range) of
        true ->
            [Entry] = ets:lookup(Tid, Idx),
            Entry;
        false when Prev == undefined->
            undefined;
        false ->
            lookup(Idx, Prev)
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
lookup_term(Idx, #?MODULE{prev = #?MODULE{} = Prev}) ->
    lookup_term(Idx, Prev);
lookup_term(_Idx, _State) ->
    undefined.

-spec tid_for(ra:index(), ra_term(), state()) ->
    undefined | ets:tid().
tid_for(_Idx, _Term, undefined) ->
    undefined;
tid_for(Idx, Term, State) ->
    Tid = tid(State),
    case ets:lookup_element(Tid, Idx, 2, undefined) of
        Term ->
            Tid;
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

-spec delete(delete_spec()) ->
    non_neg_integer().
delete(undefined) ->
    0;
delete({range, Tid, {Start, End}}) ->
    NumToDelete = End - Start + 1,
    Limit = ets:info(Tid, size) div 2,
    %% check if there is an entry below the start of the deletion range,
    %% if there is we've missed a segment event at some point and need
    %% to perform a mop-up delete with `<`, irrespective of how many entries
    LowerExists = ets:member(Tid, Start-1),
    case NumToDelete > Limit orelse LowerExists of
        true ->
            %% more than half the table is to be deleted
            delete({'<', Tid, End + 1});
        false ->
            delete(Start, End, Tid),
            End - Start + 1
    end;
delete({Op, Tid, Idx})
  when is_integer(Idx) and is_atom(Op) ->
    DelSpec = [{{'$1', '_', '_'}, [{'<', '$1', Idx}], [true]}],
    ets:select_delete(Tid, DelSpec);
delete({delete, Tid}) ->
    true = ets:delete(Tid),
    0.

-spec range_overlap(ra:range(), state()) ->
    {Overlap :: ra:range(), Remainder :: ra:range()}.
range_overlap(ReqRange, #?MODULE{} = State) ->
    Range = range(State),
    case ra_range:overlap(ReqRange, Range) of
        undefined ->
            {undefined, ReqRange};
        Overlap ->
            {Overlap, case ra_range:subtract(Overlap, ReqRange) of
                          [] ->
                              undefined;
                          [R] ->
                              R
                      end}
    end.

-spec range(state()) ->
    undefined | {ra:index(), ra:index()}.
range(#?MODULE{range = Range,
               prev = undefined}) ->
    Range;
range(#?MODULE{range = {_, End} = Range,
               prev = Prev}) ->
    PrevRange = ra_range:limit(End, range(Prev)),
    ra_range:add(Range, PrevRange);
range(_State) ->
    undefined.

-spec tid(state()) -> ets:tid().
tid(#?MODULE{tid = Tid}) ->
    Tid.

-spec staged(state()) -> [log_entry()].
staged(#?MODULE{staged = {_, Staged}}) ->
    Staged;
staged(#?MODULE{staged = undefined}) ->
    [].

-spec is_active(ets:tid(), state()) -> boolean().
is_active(Tid, State) ->
    Tid =:= tid(State).

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

-spec record_flushed(ets:tid(), ra:range(), state()) ->
    {delete_spec(), state()}.
record_flushed(TID = Tid, {Start, End},
               #?MODULE{tid = TID,
                        range = Range} = State) ->
    HasExtraEntries = ets:info(Tid, size) > ra_range:size(Range),
    case ?IN_RANGE(End, Range) of
        true when HasExtraEntries ->
            {{'<', Tid, End + 1},
             State#?MODULE{range = ra_range:truncate(End, Range)}};
        true ->
            {{range, Tid, {Start, End}},
             State#?MODULE{range = ra_range:truncate(End, Range)}};
        false ->
            {undefined, State}
    end;
record_flushed(_Tid, _Range, #?MODULE{prev = undefined} = State) ->
    {undefined, State};
record_flushed(Tid, Range, #?MODULE{prev = Prev0} = State) ->
    %% TODO: test many levels deep flushes
    {Spec, Prev} = record_flushed(Tid, Range, Prev0),
    case range(Prev) of
        undefined ->
            %% the prev table is now empty and can be deleted,
            {{delete, Tid}, State#?MODULE{prev = undefined}};
        _ ->
            {Spec, State#?MODULE{prev = Prev}}
    end.

-spec set_first(ra:index(), state()) ->
    {[delete_spec()], state()}.
set_first(Idx, #?MODULE{tid = Tid,
                        range = Range,
                        prev = Prev0} = State)
  when (is_tuple(Range) andalso
        Idx > element(1, Range)) orelse
       Range == undefined ->
    {PrevSpecs, Prev} = case Prev0 of
                            undefined ->
                                {[], undefined};
                            _ ->
                                case set_first(Idx, Prev0) of
                                    {[{range, PTID, _} | Rem],
                                     #?MODULE{tid = PTID} = P} = Res ->
                                        %% set_first/2 returned a range spec for
                                        %% prev and prev is now empty,
                                        %% upgrade to delete spec of whole tid
                                        case range(P) of
                                            undefined ->
                                                {[{delete, tid(P)} | Rem],
                                                 prev(P)};
                                            _ ->
                                                Res
                                        end;
                                    Res ->
                                        Res
                                end
                        end,
    Specs = case Range of
                {Start, End} ->
                    [{range, Tid, {Start, min(Idx - 1, End)}} | PrevSpecs];
                undefined ->
                    PrevSpecs
            end,
    {Specs,
     State#?MODULE{range = ra_range:truncate(Idx - 1, Range),
                   prev = Prev}};
set_first(_Idx, State) ->
    {[], State}.


%% internal

update_range_end(Idx, {Start, End})
  when Idx =< End orelse
       Idx == End + 1 ->
    {Start, Idx};
update_range_end(Idx, undefined) ->
    {Idx, Idx}.

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

