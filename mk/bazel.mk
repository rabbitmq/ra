define BAZELRC
build --@bazel-erlang//:erlang_home=$(shell dirname $$(dirname $$(which erl)))
build --@bazel-erlang//:erlang_version=$(shell erl -eval '{ok, Version} = file:read_file(filename:join([code:root_dir(), "releases", erlang:system_info(otp_release), "OTP_VERSION"])), io:fwrite(Version), halt().' -noshell)

build --incompatible_strict_action_env

build --test_strategy=exclusive
endef

export BAZELRC
.bazelrc:
	echo "$$BAZELRC" > $@
