PROJECT = ra
# PROJECT_DESCRIPTION = Experimental raft library
# PROJECT_VERSION = 0.1.0
# PROJECT_MOD = ra_app

##NB: ra uses an src/ra.app.src file

ESCRIPT_NAME = ra_fifo_cli
ESCRIPT_EMU_ARGS = -noinput -setcookie ra_fifo_cli

dep_gen_batch_server = hex 0.8.6
dep_aten = hex 0.5.7
DEPS = aten gen_batch_server

TEST_DEPS = proper meck eunit_formatters looking_glass inet_tcp_proxy

BUILD_DEPS = elvis_mk

LOCAL_DEPS = sasl crypto
dep_elvis_mk = git https://github.com/inaka/elvis.mk.git master
dep_looking_glass = git https://github.com/rabbitmq/looking-glass.git master
dep_inet_tcp_proxy = git https://github.com/rabbitmq/inet_tcp_proxy

DEP_PLUGINS = elvis_mk

PLT_APPS += eunit proper syntax_tools erts kernel stdlib common_test inets aten mnesia ssh ssl meck looking_glass gen_batch_server inet_tcp_proxy

EDOC_OUTPUT = docs
EDOC_OPTS = {pretty_printer, erl_pp}, {sort_functions, false}

all::

escript-zip::
	mkdir -p $(DEPS_DIR)/elvis_mk/ebin

DIALYZER_OPTS += --src -r test
EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}
include $(if $(ERLANG_MK_FILENAME),$(ERLANG_MK_FILENAME),erlang.mk)


check-rabbitmq-components.mk:
	true

RABBITMQ_UPSTREAM_FETCH_URL ?= https://github.com/rabbitmq/aten.git

.PHONY: show-upstream-git-fetch-url

show-upstream-git-fetch-url:
	@echo $(RABBITMQ_UPSTREAM_FETCH_URL)

include mk/bazel.mk
