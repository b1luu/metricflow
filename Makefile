GREMLINS_VERSION := v0.6.0
GREMLINS_CONFIG  := .gremlins.yaml
GREMLINS_REPORT  := gremlins-report.json
DIFF_BASE        ?= main
GREMLINS_CMD     := go run github.com/go-gremlins/gremlins/cmd/gremlins@$(GREMLINS_VERSION) unleash --config=$(GREMLINS_CONFIG) --output=$(GREMLINS_REPORT)

UNAME_S := $(shell uname -s 2>/dev/null)

.PHONY: test mutation mutation-diff

test:
	go test ./... -count=1

mutation: test
	go clean -testcache
ifeq ($(UNAME_S),Linux)
	ulimit -v 2621440 && $(GREMLINS_CMD)
else
	$(GREMLINS_CMD)
endif

mutation-diff: test
	go clean -testcache
ifeq ($(UNAME_S),Linux)
	ulimit -v 2621440 && $(GREMLINS_CMD) --diff=$(DIFF_BASE)
else
	$(GREMLINS_CMD) --diff=$(DIFF_BASE)
endif
