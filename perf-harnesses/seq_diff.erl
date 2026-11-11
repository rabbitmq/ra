-module(seq_diff).

%% Differential test: original ra_seq (ra_seq_orig) vs patched (ra_seq)
-export([run/0]).

run() ->
    rand:seed(exsss, {1, 2, 3}),
    ok = check_add(200000),
    ok = check_remove_prefix(200000),
    io:format("all differential checks passed~n"),
    ok.

rand_seq() ->
    N = rand:uniform(12),
    L = lists:usort([rand:uniform(60) || _ <- lists:seq(1, N)]),
    ra_seq:from_list(L).

rand_seq_from(L) ->
    ra_seq:from_list(L).

check_add(0) -> ok;
check_add(N) ->
    A = rand_seq(),
    B = rand_seq(),
    R1 = ra_seq_orig:add(A, B),
    R2 = ra_seq:add(A, B),
    case R1 == R2 of
        true -> ok;
        false ->
            io:format("DIFF add(~w,~w): orig ~w new ~w~n", [A, B, R1, R2]),
            erlang:error(repr_diff)
    end,
    %% the merged sequence must remain a valid, appendable ra_seq
    Last = ra_seq:last(R2),
    _ = ra_seq:append(Last + 1, R2),
    check_add(N - 1).

check_remove_prefix(0) -> ok;
check_remove_prefix(N) ->
    Seq = rand_seq(),
    All = ra_seq:expand(Seq),
    %% build a prefix candidate: sometimes a real prefix, sometimes junk
    Prefix = case rand:uniform(3) of
                 1 ->
                     %% genuine prefix of Seq
                     K = rand:uniform(max(1, length(All))),
                     rand_seq_from(lists:sublist(All, K));
                 2 ->
                     %% prefix with extra lower indexes not in Seq
                     K = rand:uniform(max(1, length(All))),
                     rand_seq_from(lists:usort(lists:sublist(All, K) ++
                                               [rand:uniform(60)]));
                 3 ->
                     rand_seq()
             end,
    R1 = ra_seq_orig:remove_prefix(Prefix, Seq),
    R2 = ra_seq:remove_prefix(Prefix, Seq),
    Norm = fun ({ok, S}) -> {ok, lists:sort(ra_seq:expand(S))};
               (E) -> E
           end,
    case Norm(R1) == Norm(R2) of
        true -> ok;
        false ->
            io:format("DIFF remove_prefix(~w, ~w): orig ~w new ~w~n",
                      [Prefix, Seq, R1, R2]),
            erlang:error(diff)
    end,
    check_remove_prefix(N - 1).
