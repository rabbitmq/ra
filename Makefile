PROJECT = ra
PROJECT_DESCRIPTION = Experimental raft library
PROJECT_VERSION = 0.1.0
PROJECT_MOD = ra_app

define PROJECT_ENV
[
	{data_dir, "/tmp/ra_data"}
]
endef

ESCRIPT_NAME = ra_fifo_cli
ESCRIPT_EMU_ARGS = -noinput -setcookie ra_fifo_cli

dep_aten = git https://github.com/rabbitmq/aten.git master
DEPS = aten

TEST_DEPS = proper meck eunit_formatters looking_glass rabbitmq_ct_helpers

BUILD_DEPS = elvis_mk

LOCAL_DEPS = sasl crypto
dep_elvis_mk = git https://github.com/inaka/elvis.mk.git master
dep_looking_glass = git https://github.com/rabbitmq/looking-glass.git master
dep_rabbitmq_ct_helpers = git https://github.com/rabbitmq/rabbitmq-ct-helpers

DEP_PLUGINS = elvis_mk

PLT_APPS += eunit proper syntax_tools erts kernel stdlib common_test inets aten looking_glass mnesia ssh ssl xref

EDOC_OUTPUT = docs

all::

escript-zip::
	mkdir -p $(DEPS_DIR)/elvis_mk/ebin

DIALYZER_OPTS += --src -r test
EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}
include erlang.mk

shell: app

check-rabbitmq-components.mk:
	true

RABBITMQ_UPSTREAM_FETCH_URL ?= https://github.com/rabbitmq/aten.git

.PHONY: show-upstream-git-fetch-url

show-upstream-git-fetch-url:
	@echo $(RABBITMQ_UPSTREAM_FETCH_URL)
