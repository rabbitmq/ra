-module(ra_log_memtbl_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-export([
         ]).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     basics,
     perf
    ].

groups() ->
    [{tests, [], all_tests()}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%%===================================================================
%%% Test cases
%%%===================================================================

basics(_Config) ->
    Tid = ets:new(t1, [set, public]),
    Mt0 = ra_log_memtbl:init(Tid),
    Mt1 = lists:foldl(
            fun (I, Acc) ->
                    ra_log_memtbl:insert({I, I, <<"banana">>}, Acc)
            end, Mt0, lists:seq(1, 1000)),
    {Spec, Mt} = ra_log_memtbl:set_first(500, Mt1),
    499 = ra_log_memtbl:delete(Spec, Mt),
    ?assertEqual({500, 1000}, ra_log_memtbl:range(Mt)),
    ?assertEqual(501, ets:info(Tid, size)),
    ok.

perf(_Config) ->
    Num = 1_000_000,
    Tables = [ra_log_memtbl:init(ets:new(t1, [set, public])),
              ra_log_memtbl:init(ets:new(t2, [ordered_set, public])),
              ra_log_memtbl:init(ets:new(t3, [ordered_set, public,
                                              {write_concurrency, true}]))
             ],

    InsertedMts =
    [begin
         {Taken, MtOut} = timer:tc(?MODULE, insert_n, [0, Num, <<"banana">>, Mt]),
         #{name := Name, size := Size} = ra_log_memtbl:info(MtOut),
         ct:pal("~s insert ~b entries took ~bms",
                [Name, Size, Taken div 1000]),
         MtOut
     end || Mt <- Tables
    ],

    [begin
         {Taken, MtOut} = timer:tc(?MODULE, read_keys_n, [0, Num, [0, Num-1], Mt]),
         #{name := Name} = ra_log_memtbl:info(MtOut),
         ct:pal("~s read_keys took ~bms",
                [Name, Taken div 1000]),
         ok
     end || Mt <- Tables
    ],

    From = trunc(Num * 0.9),
    To = Num - 2,
    [begin
         {Taken, Read} = timer:tc(?MODULE, read_n, [From, To, [], Mt]),
         #{name := Name} = ra_log_memtbl:info(Mt),
         ct:pal("~s read_n ~b took ~bms",
                [Name, length(Read), Taken div 1000]),
         ok
     end || Mt <- Tables
    ],

    Indexes = lists:seq(1, 1000, 2),
    [begin
         Fun = fun () -> _ = ra_log_memtbl:get_items(Indexes, Mt) end,
         {Taken, _Read} = timer:tc(?MODULE, do_n, [0, 100, Fun]),
         #{name := Name} = ra_log_memtbl:info(Mt),
         ct:pal("~s read_sparse ~b took ~bms",
                [Name, 0, Taken div 1000]),
         ok
     end || Mt <- Tables
    ],
    % RangeSpec = [{{'$1','_'},
    %               [{'andalso',{'>=','$1',From},{'=<','$1',To}}],
    %               ['$1']}],
    % [begin
    %      {Taken, Selected} = timer:tc(ets, select, [Tid, RangeSpec]),
    %      ct:pal("~s select_range ~b entries took ~bms",
    %             [Name, length(Selected), Taken div 1000]),
    %      ok
    %  end || {Name, Tid} <- Tables
    % ],

    % [begin
    %      {Taken, MtOut} = timer:tc(?MODULE, delete_n, [0, From, Mt]),
    %      #{name := Name, size := Size} = ra_log_memtbl:info(MtOut),
    %      ct:pal("~s delete_n size left ~b took ~bms",
    %             [Name, Size, Taken div 1000]),
    %      ok
    %  end || Mt <- Tables
    % ],

    DelTo = (trunc(Num * 0.9)),
    % DelSpec = [{{'$1', '_'}, [], [{'<', '$1', DelTo}]}],
    [begin
         {Spec, _} = ra_log_memtbl:set_first(DelTo-1, Mt),
         {Taken, Deleted} = timer:tc(ra_log_memtbl, delete, [Spec, Mt]),
          #{name := Name, size := Size} = ra_log_memtbl:info(Mt),
         ct:pal("~s size ~b select_delete ~b entries took ~bms Spec ~p",
                [Name, Size, Deleted, Taken div 1000, Spec]),
         ok
     end || Mt <- InsertedMts
    ],

    ok.


%%% Util

read_n(N, N, Acc, _Mt) ->
    Acc;
read_n(K, N, Acc, Mt) ->
    {K, _, _} = X = ra_log_memtbl:lookup(K, Mt),
    read_n(K+2, N, [X | Acc], Mt).

read_keys_n(N, N, _Keys, Mt) ->
    Mt;
read_keys_n(K, N, Keys, Mt) ->
    [{_, _, _} = ra_log_memtbl:lookup(Key, Mt) || Key <- Keys],
    read_keys_n(K+1, N, Keys, Mt).

insert_n(N, N, _Data, Mt) ->
    Mt;
insert_n(K, N, Data, Mt) ->
    insert_n(K+1, N, Data,
             ra_log_memtbl:insert({K, 42, Data}, Mt)).

delete_n(N, N, Mt) ->
    Mt;
delete_n(K, N, Mt) ->
    delete_n(K+1, N, ra_log_memtbl:delete(K, Mt)).

do_n(N, N, _Fun) ->
    ok;
do_n(N, To, Fun) ->
    Fun(),
    do_n(N+1, To, Fun).
