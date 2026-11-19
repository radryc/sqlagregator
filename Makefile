SHELL := /bin/bash
MODULES := . ./cmd/sqlag ./drivers/clickhouse ./drivers/bigquery ./drivers/cloudwatchlogsinsights ./drivers/omni

.PHONY: test tidy vet build fmt

test:
	@for m in $(MODULES); do echo "== $$m"; (cd $$m && go test ./...) || exit 1; done

tidy:
	@for m in $(MODULES); do (cd $$m && go mod tidy) || exit 1; done

vet:
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done

build:
	cd cmd/sqlag && go build -o ../../sqlag .

fmt:
	gofmt -w $$(find . -name '*.go' -not -path './.git/*')
