-module(ra_log_file_segment_writer_SUITE).
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
     receive_segments
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

receive_segments(Config) ->
    Dir = ?config(wal_dir, Config),
    {ok, TblWriterPid} = ra_log_file_segment_writer:start_link(#{data_dir => Dir}),
    {registered_name, Self} = erlang:process_info(self(), registered_name),
    % fake up a mem segment for Self
    Entries = [{1, 42, a}, {2, 42, b}, {3, 43, c}],
    Tid = make_mem_table(Self, Entries),
    MemTables = [{Self, 1, 3, Tid}],
    WalFile = filename:join(Dir, "recieve_segments.wal"),
    ok = file:write_file(WalFile, <<"waldata">>),
    ok = ra_log_file_segment_writer:accept_mem_tables(MemTables, WalFile),
    receive
        {ra_log_event, {new_segments, [{1, 3, Tid, SegmentFile}]}} ->
            {ok, Seg} = ra_log_file_segment:open(SegmentFile, #{mode => read}),
            % assert Entries have been fully transferred
            Entries = [ {I, T, binary_to_term(B)}
                        || {I, T, B} <- ra_log_file_segment:read(Seg, 1, 3)]
    after 3000 ->
              throw(ra_log_event_timeout)
    end,

    % assert wal file has been deleted.
    false = filelib:is_file(WalFile),
    ok = gen_server:stop(TblWriterPid),

    ok.

receive_segments_append(_Config) ->
    % append to a previously written dets segment
    not_impl.

receive_segments_overwrite(_Config) ->
    % append indexes 1 - 10
    % then write indexes 5 - 20
    % validate 1 - 20 have the expected values
    not_impl.

recieve_segments_rollover(_Config) ->
    % configure max dets segment size
    % receive then receive again to breach segment size limit
    % ensure dets segment is closed and writer id notified: {log_event, {segment_closed, Path}}
    not_impl.

recieve_segments_rollover_overwrite(_Config) ->
    % create rolled over segment for indexes 10 - 20
    % then write 1 - 30
    % TODO: what should happen here
    % who shouold clean up the first segment
    not_impl.


compaction(_Config) ->
    not_impl.


%%% private

make_mem_table(Name, Entries) ->
    Tid = ets:new(Name, []),
    [ets:insert(Tid, E) || E <- Entries],
    Tid.
