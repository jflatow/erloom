export ERLANG_MK ?= $(CURDIR)/erlang.mk

PROJECT = erloom
PROJECT_DESCRIPTION = Looms in Erlang
PROJECT_VERSION = 0.0.0
PROJECT_MOD = erloom

DEPS       = erlkit
dep_erlkit = git https://github.com/jflatow/erlkit.git

all:: $(ERLANG_MK)
$(ERLANG_MK):
	curl https://erlang.mk/erlang.mk | make -f -

include $(ERLANG_MK)
