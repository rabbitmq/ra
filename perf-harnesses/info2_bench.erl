-module(info2_bench).

%% Finding 6: ra_log_segment:info/2 is O(records x live-runs) because it
%% calls ra_seq:in/2 per index record. It also builds an `indexes' ra_seq
%% that no caller consumes.
%%
%% Variants:
%%   A current  - ra_seq:in/2 per record + build indexes seq
%%   B no-idx   - ra_seq:in/2 per record, drop the dead indexes seq
%%   C merge    - merge scan against the live runs, drop the indexes seq

-export([run/0]).

-define(REC, 32).

t(Name, Fun, N) ->
    _ = Fun(),
    erlang:garbage_collect(),
    R0 = element(2, process_info(self(), reductions)),
    {Time, _} = timer:tc(fun () -> loop(Fun, N) end),
    R1 = element(2, process_info(self(), reductions)),
    io:format("  ~-40s ~10.1f us ~12.1f reds~n",
              [Name, Time / N, (R1 - R0) / N]),
    Time / N.

loop(_Fun, 0) -> ok;
loop(Fun, N) -> _ = Fun(), loop(Fun, N - 1).

run() ->
    NumRecs = 4096,
    IdxBin = make_index(NumRecs),
    io:format("~n=== info/2 index pass over ~b records ===~n", [NumRecs]),
    [begin
         Live = live_seq(NumRecs, Shape),
         io:format("~n-- live: ~s (~b runs, ~b indexes) --~n",
                   [Shape, length(Live), ra_seq:length(Live)]),
         A = t("A current (ra_seq:in + indexes seq)",
               fun () -> a(IdxBin, Live) end, iters(Shape)),
         B = t("B drop the dead indexes seq",
               fun () -> b(IdxBin, Live) end, iters(Shape)),
         C = t("C merge scan, no indexes seq",
               fun () -> c(IdxBin, Live) end, iters(Shape)),
         %% all three must agree on live_size
         {_, _, _, SA} = a(IdxBin, Live),
         {_, _, _, SB} = b(IdxBin, Live),
         {_, _, _, SC} = c(IdxBin, Live),
         true = (SA == SB) andalso (SB == SC),
         io:format("  live_size agrees (~b);  A->B ~.1fx, A->C ~.1fx~n",
                   [SA, A / B, A / C])
     end || Shape <- [contiguous_all, contiguous_half, every_other,
                      every_third, sparse_100]],
    ok.

iters(every_other) -> 100;
iters(every_third) -> 100;
iters(_) -> 2000.

live_seq(N, contiguous_all) ->
    ra_seq:from_list(lists:seq(1, N));
live_seq(N, contiguous_half) ->
    ra_seq:from_list(lists:seq(1, N div 2));
live_seq(N, every_other) ->
    ra_seq:from_list([I || I <- lists:seq(1, N), I rem 2 == 0]);
live_seq(N, every_third) ->
    ra_seq:from_list([I || I <- lists:seq(1, N), I rem 3 == 0]);
live_seq(N, sparse_100) ->
    ra_seq:from_list([I || I <- lists:seq(1, N), I rem 41 == 0]).

make_index(N) ->
    iolist_to_binary(
      [<<I:64/unsigned, 1:64/unsigned, (1000 + I * 300):64/unsigned,
         300:32/unsigned, 0:32/integer>> || I <- lists:seq(1, N)]).

%% ---- A: current shape
a(Bin, Live) ->
    a(Bin, 0, 0, 0, undefined, [], 0, Live).

a(Bin, Off, Num, LastIdx, Range, IdxAcc, LiveSize, Live) ->
    case dec(Bin, Off) of
        eof ->
            {Num, Range, ra_seq:from_list(lists:reverse(IdxAcc)), LiveSize};
        {Idx, Offset, Length} ->
            IdxAcc1 = case Idx < LastIdx of
                          true -> lists:dropwhile(fun (I) -> I > Idx end,
                                                  IdxAcc);
                          false -> IdxAcc
                      end,
            LiveSize1 = case ra_seq:in(Idx, Live) of
                            true -> LiveSize + Length;
                            false -> LiveSize
                        end,
            a(Bin, Off + ?REC, Num + 1, Idx, upd(Range, Idx),
              [Idx | IdxAcc1], LiveSize1, Live)
    end.

%% ---- B: drop the unused indexes seq
b(Bin, Live) ->
    b(Bin, 0, 0, undefined, 0, Live).

b(Bin, Off, Num, Range, LiveSize, Live) ->
    case dec(Bin, Off) of
        eof ->
            {Num, Range, undefined, LiveSize};
        {Idx, _Offset, Length} ->
            LiveSize1 = case ra_seq:in(Idx, Live) of
                            true -> LiveSize + Length;
                            false -> LiveSize
                        end,
            b(Bin, Off + ?REC, Num + 1, upd(Range, Idx), LiveSize1, Live)
    end.

%% ---- C: merge scan. Live runs ascending, cursor advances with the records.
c(Bin, Live) ->
    Asc = lists:reverse(Live),
    c(Bin, 0, 0, 0, undefined, 0, Asc, Asc).

c(Bin, Off, Num, LastIdx, Range, LiveSize, Cur0, Asc) ->
    case dec(Bin, Off) of
        eof ->
            {Num, Range, undefined, LiveSize};
        {Idx, _Offset, Length} ->
            %% a backwards index means the segment has overwrites, restart
            %% the cursor rather than give a wrong answer
            Cur1 = case Idx < LastIdx of
                       true -> Asc;
                       false -> Cur0
                   end,
            Cur = advance(Idx, Cur1),
            LiveSize1 = case at(Idx, Cur) of
                            true -> LiveSize + Length;
                            false -> LiveSize
                        end,
            c(Bin, Off + ?REC, Num + 1, Idx, upd(Range, Idx), LiveSize1,
              Cur, Asc)
    end.

%% drop live runs that end below Idx
advance(Idx, [E | Rem]) ->
    case rend(E) < Idx of
        true -> advance(Idx, Rem);
        false -> [E | Rem]
    end;
advance(_Idx, []) ->
    [].

at(_Idx, []) -> false;
at(Idx, [E | _]) -> rstart(E) =< Idx.

rend({_, E}) -> E;
rend(I) -> I.
rstart({S, _}) -> S;
rstart(I) -> I.

upd(undefined, Idx) -> {Idx, Idx};
upd({F, _}, Idx) -> {min(F, Idx), Idx}.

dec(Bin, Off) when byte_size(Bin) >= Off + ?REC ->
    case Bin of
        <<_:Off/binary, 0:64, 0:64, 0:64, 0:32, 0:32/integer, _/binary>> ->
            eof;
        <<_:Off/binary, Idx:64/unsigned, _T:64/unsigned, O:64/unsigned,
          L:32/unsigned, _C:32/integer, _/binary>> ->
            {Idx, O, L}
    end;
dec(_, _) ->
    eof.
