-module(ra_log_file_table_writer_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%%
%%

all() ->
    [
     {group, tests}
    ].


all_tests() ->
    [
     receive_tables
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(tests, Config) ->
    Config.

end_per_group(tests, Config) ->
    Config.

init_per_testcase(TestCase, Config) ->
    PrivDir = ?config(priv_dir, Config),
    Dir = filename:join(PrivDir, TestCase),
    file:make_dir(Dir),
    register(TestCase, self()),
    [{test_case, TestCase}, {wal_dir, Dir} | Config].

receive_tables(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_file_table_writer:start_link(#{data_dir => Dir}),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    % fake up a mem table for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(Self, Entries),
    MemTables = [{Self, 1, 3, Tid}],
    WalFile = filename:join(Dir, "recieve_tables.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_file_table_writer:receive_tables(MemTables, WalFile),
    receive
        {log_event, {new_tables, [{1, 3, Tid, {DetsName, DetsFile}}]}} ->
            {ok, DetsName} = dets:open_file(DetsName, [{file, DetsFile}]),
            % assert Entries have been fully transferred
            Entries = lists:sort(dets:match_object(DetsName, '_'))
    after 3000 ->
              throw(new_tables_timeout)
    end,

    % assert wal file has been deleted.
    false = filelib:is_file(WalFile),
    ok = gen_server:stop(TblWriterPid),

    ok.

receive_tables_append(_Config) ->
    % append to a previously written dets table
    not_impl.

receive_tables_overwrite(_Config) ->
    % append indexes 1 - 10
    % then write indexes 5 - 20
    % validate 1 - 20 have the expected values
    not_impl.

recieve_tables_rollover(_Config) ->
    % configure max dets table size
    % receive then receive again to breach table size limit
    % ensure dets table is closed and writer id notified: {log_event, {table_closed, Path}}
    not_impl.

recieve_tables_rollover_overwrite(_Config) ->
    % create rolled over table for indexes 10 - 20
    % then write 1 - 30
    % TODO: what should happen here
    % who shouold clean up the first table
    not_impl.


compaction(_Config) ->
    not_impl.


%%% private

make_mem_table(Name, Entries) ->
    Tid = ets:new(Name, []),
    [ets:insert(Tid, E) || E <- Entries],
    Tid.
