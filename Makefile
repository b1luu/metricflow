GREMLINS_VERSION := v0.6.0
GREMLINS_REPORT  := gremlins-report.json
GOBIN            := $(shell go env GOBIN)
ifeq ($(GOBIN),)
GOBIN            := $(shell go env GOPATH)/bin
endif
GREMLINS         := $(GOBIN)/gremlins

.PHONY: test mutation

test:
	go test ./... -count=1

mutation: test
	go install github.com/go-gremlins/gremlins/cmd/gremlins@$(GREMLINS_VERSION)
	go clean -testcache
	"$(GREMLINS)" unleash --output=$(GREMLINS_REPORT)
