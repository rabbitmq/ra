load("@rules_erlang//:erlang_bytecode2.bzl", "erlang_bytecode")
load("@rules_erlang//:filegroup.bzl", "filegroup")

def all_beam_files(name = "all_beam_files"):
    filegroup(
        name = "beam_files",
        srcs = ["ebin/ra.beam", "ebin/ra_app.beam", "ebin/ra_bench.beam", "ebin/ra_counters.beam", "ebin/ra_dbg.beam", "ebin/ra_directory.beam", "ebin/ra_env.beam", "ebin/ra_file_handle.beam", "ebin/ra_flru.beam", "ebin/ra_leaderboard.beam", "ebin/ra_lib.beam", "ebin/ra_log.beam", "ebin/ra_log_ets.beam", "ebin/ra_log_meta.beam", "ebin/ra_log_pre_init.beam", "ebin/ra_log_reader.beam", "ebin/ra_log_segment.beam", "ebin/ra_log_segment_writer.beam", "ebin/ra_log_snapshot.beam", "ebin/ra_log_sup.beam", "ebin/ra_log_wal.beam", "ebin/ra_log_wal_sup.beam", "ebin/ra_machine.beam", "ebin/ra_machine_ets.beam", "ebin/ra_machine_simple.beam", "ebin/ra_metrics_ets.beam", "ebin/ra_monitors.beam", "ebin/ra_server.beam", "ebin/ra_server_proc.beam", "ebin/ra_server_sup.beam", "ebin/ra_server_sup_sup.beam", "ebin/ra_snapshot.beam", "ebin/ra_sup.beam", "ebin/ra_system.beam", "ebin/ra_system_sup.beam", "ebin/ra_systems_sup.beam"],
    )
    erlang_bytecode(
        name = "ebin_ra_app_beam",
        srcs = ["src/ra_app.erl"],
        outs = ["ebin/ra_app.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_beam",
        srcs = ["src/ra.erl"],
        outs = ["ebin/ra.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_bench_beam",
        srcs = ["src/ra_bench.erl"],
        outs = ["ebin/ra_bench.beam"],
        app_name = "ra",
        beam = ["ebin/ra_machine.beam"],
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_counters_beam",
        srcs = ["src/ra_counters.erl"],
        outs = ["ebin/ra_counters.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_dbg_beam",
        srcs = ["src/ra_dbg.erl"],
        outs = ["ebin/ra_dbg.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_directory_beam",
        srcs = ["src/ra_directory.erl"],
        outs = ["ebin/ra_directory.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_env_beam",
        srcs = ["src/ra_env.erl"],
        outs = ["ebin/ra_env.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_file_handle_beam",
        srcs = ["src/ra_file_handle.erl"],
        outs = ["ebin/ra_file_handle.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_flru_beam",
        srcs = ["src/ra_flru.erl"],
        outs = ["ebin/ra_flru.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_leaderboard_beam",
        srcs = ["src/ra_leaderboard.erl"],
        outs = ["ebin/ra_leaderboard.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_lib_beam",
        srcs = ["src/ra_lib.erl"],
        outs = ["ebin/ra_lib.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_log_beam",
        srcs = ["src/ra_log.erl"],
        outs = ["ebin/ra_log.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_log_ets_beam",
        srcs = ["src/ra_log_ets.erl"],
        outs = ["ebin/ra_log_ets.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_log_meta_beam",
        srcs = ["src/ra_log_meta.erl"],
        outs = ["ebin/ra_log_meta.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
        deps = ["@gen_batch_server//:erlang_app"],
    )
    erlang_bytecode(
        name = "ebin_ra_log_pre_init_beam",
        srcs = ["src/ra_log_pre_init.erl"],
        outs = ["ebin/ra_log_pre_init.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_log_reader_beam",
        srcs = ["src/ra_log_reader.erl"],
        outs = ["ebin/ra_log_reader.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_log_segment_beam",
        srcs = ["src/ra_log_segment.erl"],
        outs = ["ebin/ra_log_segment.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_log_segment_writer_beam",
        srcs = ["src/ra_log_segment_writer.erl"],
        outs = ["ebin/ra_log_segment_writer.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_log_snapshot_beam",
        srcs = ["src/ra_log_snapshot.erl"],
        outs = ["ebin/ra_log_snapshot.beam"],
        app_name = "ra",
        beam = ["ebin/ra_snapshot.beam"],
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_log_sup_beam",
        srcs = ["src/ra_log_sup.erl"],
        outs = ["ebin/ra_log_sup.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_log_wal_beam",
        srcs = ["src/ra_log_wal.erl"],
        outs = ["ebin/ra_log_wal.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
        deps = ["@gen_batch_server//:erlang_app"],
    )
    erlang_bytecode(
        name = "ebin_ra_log_wal_sup_beam",
        srcs = ["src/ra_log_wal_sup.erl"],
        outs = ["ebin/ra_log_wal_sup.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_machine_beam",
        srcs = ["src/ra_machine.erl"],
        outs = ["ebin/ra_machine.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_machine_ets_beam",
        srcs = ["src/ra_machine_ets.erl"],
        outs = ["ebin/ra_machine_ets.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_machine_simple_beam",
        srcs = ["src/ra_machine_simple.erl"],
        outs = ["ebin/ra_machine_simple.beam"],
        app_name = "ra",
        beam = ["ebin/ra_machine.beam"],
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_metrics_ets_beam",
        srcs = ["src/ra_metrics_ets.erl"],
        outs = ["ebin/ra_metrics_ets.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_monitors_beam",
        srcs = ["src/ra_monitors.erl"],
        outs = ["ebin/ra_monitors.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_server_beam",
        srcs = ["src/ra_server.erl"],
        outs = ["ebin/ra_server.beam"],
        hdrs = ["src/ra.hrl", "src/ra_server.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_server_proc_beam",
        srcs = ["src/ra_server_proc.erl"],
        outs = ["ebin/ra_server_proc.beam"],
        hdrs = ["src/ra.hrl", "src/ra_server.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_server_sup_beam",
        srcs = ["src/ra_server_sup.erl"],
        outs = ["ebin/ra_server_sup.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_server_sup_sup_beam",
        srcs = ["src/ra_server_sup_sup.erl"],
        outs = ["ebin/ra_server_sup_sup.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_snapshot_beam",
        srcs = ["src/ra_snapshot.erl"],
        outs = ["ebin/ra_snapshot.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_sup_beam",
        srcs = ["src/ra_sup.erl"],
        outs = ["ebin/ra_sup.beam"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_system_beam",
        srcs = ["src/ra_system.erl"],
        outs = ["ebin/ra_system.beam"],
        hdrs = ["src/ra.hrl", "src/ra_server.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_system_sup_beam",
        srcs = ["src/ra_system_sup.erl"],
        outs = ["ebin/ra_system_sup.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )
    erlang_bytecode(
        name = "ebin_ra_systems_sup_beam",
        srcs = ["src/ra_systems_sup.erl"],
        outs = ["ebin/ra_systems_sup.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:erlc_opts",
    )

def all_test_beam_files(name = "all_test_beam_files"):
    filegroup(
        name = "test_beam_files",
        testonly = True,
        srcs = ["test/ra.beam", "test/ra_app.beam", "test/ra_bench.beam", "test/ra_counters.beam", "test/ra_dbg.beam", "test/ra_directory.beam", "test/ra_env.beam", "test/ra_file_handle.beam", "test/ra_flru.beam", "test/ra_leaderboard.beam", "test/ra_lib.beam", "test/ra_log.beam", "test/ra_log_ets.beam", "test/ra_log_meta.beam", "test/ra_log_pre_init.beam", "test/ra_log_reader.beam", "test/ra_log_segment.beam", "test/ra_log_segment_writer.beam", "test/ra_log_snapshot.beam", "test/ra_log_sup.beam", "test/ra_log_wal.beam", "test/ra_log_wal_sup.beam", "test/ra_machine.beam", "test/ra_machine_ets.beam", "test/ra_machine_simple.beam", "test/ra_metrics_ets.beam", "test/ra_monitors.beam", "test/ra_server.beam", "test/ra_server_proc.beam", "test/ra_server_sup.beam", "test/ra_server_sup_sup.beam", "test/ra_snapshot.beam", "test/ra_sup.beam", "test/ra_system.beam", "test/ra_system_sup.beam", "test/ra_systems_sup.beam"],
    )
    erlang_bytecode(
        name = "test_ra_app_beam",
        testonly = True,
        srcs = ["src/ra_app.erl"],
        outs = ["test/ra_app.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_beam",
        testonly = True,
        srcs = ["src/ra.erl"],
        outs = ["test/ra.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_bench_beam",
        testonly = True,
        srcs = ["src/ra_bench.erl"],
        outs = ["test/ra_bench.beam"],
        app_name = "ra",
        beam = ["ebin/ra_machine.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_counters_beam",
        testonly = True,
        srcs = ["src/ra_counters.erl"],
        outs = ["test/ra_counters.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_dbg_beam",
        testonly = True,
        srcs = ["src/ra_dbg.erl"],
        outs = ["test/ra_dbg.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_directory_beam",
        testonly = True,
        srcs = ["src/ra_directory.erl"],
        outs = ["test/ra_directory.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_env_beam",
        testonly = True,
        srcs = ["src/ra_env.erl"],
        outs = ["test/ra_env.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_file_handle_beam",
        testonly = True,
        srcs = ["src/ra_file_handle.erl"],
        outs = ["test/ra_file_handle.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_flru_beam",
        testonly = True,
        srcs = ["src/ra_flru.erl"],
        outs = ["test/ra_flru.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_leaderboard_beam",
        testonly = True,
        srcs = ["src/ra_leaderboard.erl"],
        outs = ["test/ra_leaderboard.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_lib_beam",
        testonly = True,
        srcs = ["src/ra_lib.erl"],
        outs = ["test/ra_lib.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_log_beam",
        testonly = True,
        srcs = ["src/ra_log.erl"],
        outs = ["test/ra_log.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_log_ets_beam",
        testonly = True,
        srcs = ["src/ra_log_ets.erl"],
        outs = ["test/ra_log_ets.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_log_meta_beam",
        testonly = True,
        srcs = ["src/ra_log_meta.erl"],
        outs = ["test/ra_log_meta.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
        deps = ["@gen_batch_server//:erlang_app"],
    )
    erlang_bytecode(
        name = "test_ra_log_pre_init_beam",
        testonly = True,
        srcs = ["src/ra_log_pre_init.erl"],
        outs = ["test/ra_log_pre_init.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_log_reader_beam",
        testonly = True,
        srcs = ["src/ra_log_reader.erl"],
        outs = ["test/ra_log_reader.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_log_segment_beam",
        testonly = True,
        srcs = ["src/ra_log_segment.erl"],
        outs = ["test/ra_log_segment.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_log_segment_writer_beam",
        testonly = True,
        srcs = ["src/ra_log_segment_writer.erl"],
        outs = ["test/ra_log_segment_writer.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_log_snapshot_beam",
        testonly = True,
        srcs = ["src/ra_log_snapshot.erl"],
        outs = ["test/ra_log_snapshot.beam"],
        app_name = "ra",
        beam = ["ebin/ra_snapshot.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_log_sup_beam",
        testonly = True,
        srcs = ["src/ra_log_sup.erl"],
        outs = ["test/ra_log_sup.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_log_wal_beam",
        testonly = True,
        srcs = ["src/ra_log_wal.erl"],
        outs = ["test/ra_log_wal.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
        deps = ["@gen_batch_server//:erlang_app"],
    )
    erlang_bytecode(
        name = "test_ra_log_wal_sup_beam",
        testonly = True,
        srcs = ["src/ra_log_wal_sup.erl"],
        outs = ["test/ra_log_wal_sup.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_machine_beam",
        testonly = True,
        srcs = ["src/ra_machine.erl"],
        outs = ["test/ra_machine.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_machine_ets_beam",
        testonly = True,
        srcs = ["src/ra_machine_ets.erl"],
        outs = ["test/ra_machine_ets.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_machine_simple_beam",
        testonly = True,
        srcs = ["src/ra_machine_simple.erl"],
        outs = ["test/ra_machine_simple.beam"],
        app_name = "ra",
        beam = ["ebin/ra_machine.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_metrics_ets_beam",
        testonly = True,
        srcs = ["src/ra_metrics_ets.erl"],
        outs = ["test/ra_metrics_ets.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_monitors_beam",
        testonly = True,
        srcs = ["src/ra_monitors.erl"],
        outs = ["test/ra_monitors.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_server_beam",
        testonly = True,
        srcs = ["src/ra_server.erl"],
        outs = ["test/ra_server.beam"],
        hdrs = ["src/ra.hrl", "src/ra_server.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_server_proc_beam",
        testonly = True,
        srcs = ["src/ra_server_proc.erl"],
        outs = ["test/ra_server_proc.beam"],
        hdrs = ["src/ra.hrl", "src/ra_server.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_server_sup_beam",
        testonly = True,
        srcs = ["src/ra_server_sup.erl"],
        outs = ["test/ra_server_sup.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_server_sup_sup_beam",
        testonly = True,
        srcs = ["src/ra_server_sup_sup.erl"],
        outs = ["test/ra_server_sup_sup.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_snapshot_beam",
        testonly = True,
        srcs = ["src/ra_snapshot.erl"],
        outs = ["test/ra_snapshot.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_sup_beam",
        testonly = True,
        srcs = ["src/ra_sup.erl"],
        outs = ["test/ra_sup.beam"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_system_beam",
        testonly = True,
        srcs = ["src/ra_system.erl"],
        outs = ["test/ra_system.beam"],
        hdrs = ["src/ra.hrl", "src/ra_server.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_system_sup_beam",
        testonly = True,
        srcs = ["src/ra_system_sup.erl"],
        outs = ["test/ra_system_sup.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_systems_sup_beam",
        testonly = True,
        srcs = ["src/ra_systems_sup.erl"],
        outs = ["test/ra_systems_sup.beam"],
        hdrs = ["src/ra.hrl"],
        app_name = "ra",
        erlc_opts = "//:test_erlc_opts",
    )

def test_suite_beam_files(name = "test_suite_beam_files"):
    erlang_bytecode(
        name = "coordination_SUITE_beam_files",
        testonly = True,
        srcs = ["test/coordination_SUITE.erl"],
        outs = ["test/coordination_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "partitions_SUITE_beam_files",
        testonly = True,
        srcs = ["test/partitions_SUITE.erl"],
        outs = ["test/partitions_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
        deps = ["@proper//:erlang_app"],
    )
    erlang_bytecode(
        name = "ra_2_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_2_SUITE.erl"],
        outs = ["test/ra_2_SUITE.beam"],
        beam = ["ebin/ra_machine.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_SUITE.erl"],
        outs = ["test/ra_SUITE.beam"],
        hdrs = ["src/ra.hrl"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_dbg_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_dbg_SUITE.erl"],
        outs = ["test/ra_dbg_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_directory_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_directory_SUITE.erl"],
        outs = ["test/ra_directory_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_log_2_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_log_2_SUITE.erl"],
        outs = ["test/ra_log_2_SUITE.beam"],
        hdrs = ["src/ra.hrl"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_log_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_log_SUITE.erl"],
        outs = ["test/ra_log_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_log_ets_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_log_ets_SUITE.erl"],
        outs = ["test/ra_log_ets_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_log_meta_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_log_meta_SUITE.erl"],
        outs = ["test/ra_log_meta_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_log_props_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_log_props_SUITE.erl"],
        outs = ["test/ra_log_props_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
        deps = ["@proper//:erlang_app"],
    )
    erlang_bytecode(
        name = "ra_log_segment_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_log_segment_SUITE.erl"],
        outs = ["test/ra_log_segment_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_log_segment_writer_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_log_segment_writer_SUITE.erl"],
        outs = ["test/ra_log_segment_writer_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_log_snapshot_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_log_snapshot_SUITE.erl"],
        outs = ["test/ra_log_snapshot_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_log_wal_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_log_wal_SUITE.erl"],
        outs = ["test/ra_log_wal_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_machine_ets_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_machine_ets_SUITE.erl"],
        outs = ["test/ra_machine_ets_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_machine_int_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_machine_int_SUITE.erl"],
        outs = ["test/ra_machine_int_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_machine_version_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_machine_version_SUITE.erl"],
        outs = ["test/ra_machine_version_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_props_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_props_SUITE.erl"],
        outs = ["test/ra_props_SUITE.beam"],
        hdrs = ["src/ra.hrl"],
        erlc_opts = "//:test_erlc_opts",
        deps = ["@proper//:erlang_app"],
    )
    erlang_bytecode(
        name = "ra_server_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_server_SUITE.erl"],
        outs = ["test/ra_server_SUITE.beam"],
        hdrs = ["src/ra.hrl", "src/ra_server.hrl"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_snapshot_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_snapshot_SUITE.erl"],
        outs = ["test/ra_snapshot_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "ra_system_SUITE_beam_files",
        testonly = True,
        srcs = ["test/ra_system_SUITE.erl"],
        outs = ["test/ra_system_SUITE.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_consumer_beam",
        testonly = True,
        srcs = ["test/consumer.erl"],
        outs = ["test/consumer.beam"],
        hdrs = ["src/ra.hrl"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_enqueuer_beam",
        testonly = True,
        srcs = ["test/enqueuer.erl"],
        outs = ["test/enqueuer.beam"],
        hdrs = ["src/ra.hrl"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_erlang_node_helpers_beam",
        testonly = True,
        srcs = ["test/erlang_node_helpers.erl"],
        outs = ["test/erlang_node_helpers.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_nemesis_beam",
        testonly = True,
        srcs = ["test/nemesis.erl"],
        outs = ["test/nemesis.beam"],
        hdrs = ["src/ra.hrl"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_fifo_beam",
        testonly = True,
        srcs = ["test/ra_fifo.erl"],
        outs = ["test/ra_fifo.beam"],
        hdrs = ["src/ra.hrl"],
        beam = ["ebin/ra_machine.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_fifo_client_beam",
        testonly = True,
        srcs = ["test/ra_fifo_client.erl"],
        outs = ["test/ra_fifo_client.beam"],
        hdrs = ["src/ra.hrl"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_fifo_index_beam",
        testonly = True,
        srcs = ["test/ra_fifo_index.erl"],
        outs = ["test/ra_fifo_index.beam"],
        hdrs = ["src/ra.hrl"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_log_memory_beam",
        testonly = True,
        srcs = ["test/ra_log_memory.erl"],
        outs = ["test/ra_log_memory.beam"],
        hdrs = ["src/ra.hrl"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_ra_queue_beam",
        testonly = True,
        srcs = ["test/ra_queue.erl"],
        outs = ["test/ra_queue.beam"],
        beam = ["ebin/ra_machine.beam"],
        erlc_opts = "//:test_erlc_opts",
    )
    erlang_bytecode(
        name = "test_tcp_inet_proxy_helpers_beam",
        testonly = True,
        srcs = ["test/tcp_inet_proxy_helpers.erl"],
        outs = ["test/tcp_inet_proxy_helpers.beam"],
        erlc_opts = "//:test_erlc_opts",
    )

def all_srcs(name = "all_srcs"):
    filegroup(
        name = "all_srcs",
        srcs = [":public_and_private_hdrs", ":srcs"],
    )
    filegroup(
        name = "licenses",
        srcs = ["LICENSE", "LICENSE-APACHE2", "LICENSE-MPL-RabbitMQ"],
    )
    filegroup(
        name = "priv",
        srcs = [],
    )
    filegroup(
        name = "private_hdrs",
        srcs = ["src/ra.hrl", "src/ra_server.hrl"],
    )
    filegroup(
        name = "public_and_private_hdrs",
        srcs = [":private_hdrs", ":public_hdrs"],
    )
    filegroup(
        name = "public_hdrs",
        srcs = [],
    )
    filegroup(
        name = "srcs",
        srcs = ["src/ra.app.src", "src/ra.erl", "src/ra_app.erl", "src/ra_bench.erl", "src/ra_counters.erl", "src/ra_dbg.erl", "src/ra_directory.erl", "src/ra_env.erl", "src/ra_file_handle.erl", "src/ra_flru.erl", "src/ra_leaderboard.erl", "src/ra_lib.erl", "src/ra_log.erl", "src/ra_log_ets.erl", "src/ra_log_meta.erl", "src/ra_log_pre_init.erl", "src/ra_log_reader.erl", "src/ra_log_segment.erl", "src/ra_log_segment_writer.erl", "src/ra_log_snapshot.erl", "src/ra_log_sup.erl", "src/ra_log_wal.erl", "src/ra_log_wal_sup.erl", "src/ra_machine.erl", "src/ra_machine_ets.erl", "src/ra_machine_simple.erl", "src/ra_metrics_ets.erl", "src/ra_monitors.erl", "src/ra_server.erl", "src/ra_server_proc.erl", "src/ra_server_sup.erl", "src/ra_server_sup_sup.erl", "src/ra_snapshot.erl", "src/ra_sup.erl", "src/ra_system.erl", "src/ra_system_sup.erl", "src/ra_systems_sup.erl"],
    )
