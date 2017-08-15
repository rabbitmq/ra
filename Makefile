PROJECT = ra
PROJECT_DESCRIPTION = Experimental raft library
PROJECT_VERSION = 0.1.0

TEST_DEPS = proper meck eunit_formatters

BUILD_DEPS = elvis_mk looking_glass
dep_elvis_mk = git https://github.com/inaka/elvis.mk.git master
dep_looking_glass = git https://github.com/rabbitmq/looking-glass.git master

DEP_PLUGINS = elvis_mk

DIALYZER_OPTS += --apps eunit meck proper --src -r test
EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}
include erlang.mk

shell: app
