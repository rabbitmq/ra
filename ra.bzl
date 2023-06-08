load("@rules_erlang//:erlang_bytecode.bzl", "erlang_bytecode")
load("@rules_erlang//:ct.bzl", "ct_suite")

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

    erlang_bytecode(
        name = "test_helpers",
        erlc_opts = TEST_ERLC_OPTS,
        srcs = helpers,
        hdrs = hdrs,
        deps = [
            ":test_erlang_app",
        ],
        dest = "test",
        testonly = True,
    )

    for file in suites:
        name = file.replace("test/", "").replace(".erl", "")
        ct_suite(
            erlc_opts = TEST_ERLC_OPTS,
            name = name,
            runtime_deps = [
                "@gen_batch_server//:erlang_app",
                "@aten//:erlang_app",
                "@seshat//:erlang_app",
                "@inet_tcp_proxy_dist//:erlang_app",
                "@meck//:erlang_app",
            ],
            deps = [
                "@proper//:erlang_app",
            ],
            additional_hdrs = hdrs,
            additional_beam = [
                ":test_helpers",
            ],
        )
