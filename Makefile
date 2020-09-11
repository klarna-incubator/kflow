PICS=$(patsubst %.uml,%.png,$(wildcard doc/images/*.uml))

ENV_FILE=.env

MIN_COVERAGE := 75
BUILD_DIR := $(CURDIR)/_build
CONCUERROR := $(BUILD_DIR)/Concuerror/bin/concuerror
CONCUERROR_RUN := $(CONCUERROR) -x snabbkaffe_collector -x snabbkaffe -x snabbkaffe_nemesis -x snabbkaffe_sup \
	-x code -x code_server -x error_handler \
	-pa $(BUILD_DIR)/concuerror+test/lib/snabbkaffe/ebin \
	-pa $(BUILD_DIR)/concuerror+test/lib/kflow/ebin

-include $(ENV_FILE)

.PHONY: all
all: $(PICS)
	rebar3 do compile,edoc,dialyzer,xref,eunit,ct,cover -v -m $(MIN_COVERAGE)

.PHONY: clean
clean:
	rm -rf $(BUILD_DIR)
	rm .env

.PHONY: doc
doc: $(PICS)
	rebar3 edoc

doc/images/%.png: doc/images/%.uml
	plantuml $<

.PHONY: run
run: $(ENV_FILE)
	@echo "This is a test setup!"
	rebar3 as dev release
	docker-compose -f test/kflow_sysmon_receiver_SUITE_data/docker-compose.yml up -d --force-recreate --build
	@POSTGRES_KFLOW_PWD=$(POSTGRES_KFLOW_PWD) $(BUILD_DIR)/dev/rel/kflow/bin/kflow console

$(ENV_FILE):
	@mkdir -p $(BUILD_DIR)
	@echo "PROJECT_DIR=$(CURDIR)\n\
POSTGRES_PASSWORD=`pwgen -n1 16`\n\
POSTGRES_GRAFANA_PWD=`pwgen -n1 16`\n\
POSTGRES_KFLOW_PWD=`pwgen -n1 16`" > $@

.PHONY: concuerror_test
concuerror_test: $(CONCUERROR)
	rebar3 as concuerror eunit
	$(CONCUERROR_RUN) -f $(BUILD_DIR)/concuerror+test/lib/kflow/test/concuerror_tests.beam

$(CONCUERROR):
	@mkdir -p $(BUILD_DIR)
	cd _build && git clone https://github.com/parapluu/Concuerror.git
	$(MAKE) -C _build/Concuerror/

hex-publish: clean
	rebar3 as dev hex publish
