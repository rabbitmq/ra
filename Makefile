PROJECT = ra
PROJECT_DESCRIPTION = Experimental raft library
PROJECT_VERSION = 0.1.0
PROJECT_MOD = ra_app

define PROJECT_ENV
[
	{data_dir, "/var/vcap/store/ra/shared"}
]
endef

dep_aten = git https://github.com/rabbitmq/aten.git master
DEPS = aten

TEST_DEPS = proper meck eunit_formatters looking_glass

BUILD_DEPS = elvis_mk

LOCAL_DEPS = sasl crypto
dep_elvis_mk = git https://github.com/inaka/elvis.mk.git master
dep_looking_glass = git https://github.com/rabbitmq/looking-glass.git master

DEP_PLUGINS = elvis_mk

PLT_APPS += eunit meck proper syntax_tools erts kernel stdlib common_test inets aten

DIALYZER_OPTS += --src -r test
EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}
include erlang.mk

shell: app
