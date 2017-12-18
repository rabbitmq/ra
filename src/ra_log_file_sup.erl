-module(ra_log_file_sup).

-behaviour(supervisor).

%% API functions
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-spec start_link() ->
    {ok, pid()} | ignore | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, DataDir} = application:get_env(data_dir),
    SegmentMaxEntries = application:get_env(ra, segment_max_entries, 4096),
    SegWriterConf = #{data_dir => DataDir,
                      segment_conf => #{max_count => SegmentMaxEntries}},
    SegWriter = #{id => ra_log_file_segment_writer,
                  start => {ra_log_file_segment_writer, start_link, [SegWriterConf]}},
    WalConf = #{dir => DataDir},
    SupFlags = #{strategy => one_for_all, intensity => 5, period => 5},
    WalSup = #{id => ra_log_wal_sup,
               type => supervisor,
               start => {ra_log_wal_sup, start_link, [WalConf]}},
    {ok, {SupFlags, [SegWriter, WalSup]}}.
