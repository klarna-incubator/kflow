PICS=$(patsubst %.uml,%.png,$(wildcard images/*.uml))

MIN_COVERAGE := 80
BUILD_DIR := $(CURDIR)/_build
CONCUERROR := $(BUILD_DIR)/Concuerror/bin/concuerror
CONCUERROR_RUN := $(CONCUERROR) -x snabbkaffe_collector -x snabbkaffe -x snabbkaffe_nemesis -x snabbkaffe_sup \
	-x code -x code_server -x error_handler \
	-pa $(BUILD_DIR)/concuerror+test/lib/snabbkaffe/ebin \
	-pa $(BUILD_DIR)/concuerror+test/lib/kflow/ebin

.PHONY: all
all: $(PICS)
	rebar3 do compile,edoc,dialyzer,xref,eunit,ct,cover -v -m $(MIN_COVERAGE)

.PHONY: doc
doc: $(PICS)
	rebar3 edoc

images/%.png: images/%.uml
	plantuml $<

.PHONY: concuerror_test
concuerror_test: $(CONCUERROR)
	rebar3 as concuerror eunit
	$(CONCUERROR_RUN) -f $(BUILD_DIR)/concuerror+test/lib/kflow/test/concuerror_tests.beam

$(CONCUERROR):
	mkdir -p _build/
	cd _build && git clone https://github.com/parapluu/Concuerror.git
	$(MAKE) -C _build/Concuerror/
