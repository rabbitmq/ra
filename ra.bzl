load("@bazel-erlang//:bazel_erlang_lib.bzl", "erlc")
load("@bazel-erlang//:ct.bzl", "ct_suite")

TEST_ERLC_OPTS = [
    "-DTEST",
    "+debug_info",
    "+nowarn_export_all",
]

def ra_suites():
    suites = native.glob(["test/*_SUITE.erl"])
    helpers = native.glob(["test/*.erl"], exclude = suites)

    hdrs = [
        "src/ra.hrl",
        "src/ra_server.hrl",
    ]

    erlc(
        name = "test_helpers",
        srcs = helpers,
        hdrs = hdrs,
        deps = [
            ":test_bazel_erlang_lib",
        ],
        testonly = True,
    )

    for file in suites:
        name = file.replace("test/", "").replace(".erl", "")
        ct_suite(
            erlc_opts = TEST_ERLC_OPTS,
            name = name,
            runtime_deps = [
                "@gen_batch_server//:bazel_erlang_lib",
                "@aten//:bazel_erlang_lib",
                "@inet_tcp_proxy//:bazel_erlang_lib",
                "@meck//:bazel_erlang_lib",
            ],
            deps = [
                "@proper//:bazel_erlang_lib",
            ],
            additional_hdrs = hdrs,
            additional_beam = [
                ":test_helpers",
            ],
        )
