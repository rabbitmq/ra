PROJECT = ra
PROJECT_DESCRIPTION = Experimental raft library
PROJECT_VERSION = 0.1.0

TEST_DEPS = proper

BUILD_DEPS = elvis_mk
dep_elvis_mk = git https://github.com/inaka/elvis.mk.git master

DEP_PLUGINS = elvis_mk

include erlang.mk
