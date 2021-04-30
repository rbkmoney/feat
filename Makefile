REBAR := $(shell which rebar3 2>/dev/null || which ./rebar3)

.PHONY: get_deps ensure_elvis compile lint check_format \
    format test xref clean distclean dialyze plt_update

get_deps:
	$(REBAR) get-deps

compile:
	$(REBAR) compile

lint:
	$(REBAR) lint

check_format:
	$(REBAR) fmt -c

format:
	$(REBAR) fmt -w

test:
	$(REBAR) do eunit, proper

xref:
	$(REBAR) xref

clean:
	$(REBAR) clean

distclean:
	$(REBAR) clean -a
	rm -rfv _build _builds _cache _steps _temp

dialyze:
	$(REBAR) dialyzer

plt_update:
	$(REBAR) dialyzer -u true -s false
