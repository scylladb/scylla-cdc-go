GOVERSION ?= 1.22.5
GOOS := $(shell uname | tr '[:upper:]' '[:lower:]')
GOARCH := $(shell go env GOARCH)
GOPACKAGES := $(shell go list -tags="e2e,integration" ./...)
GOBUILDPACKAGES := $(shell go list ./...)

GIT_COMMIT := $(shell git rev-list -1 HEAD)
TAG_VERSION := $(shell git describe --tags --abbrev=0)
DATE := $(shell date -u)
MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))

ifndef SCYLLA_SUBNET
	export SCYLLA_SUBNET := 10.254.254.0/24
endif
ifndef SCYLLA_SRC_URI
	export SCYLLA_SRC_URI := 10.254.254.100
endif
ifndef SCYLLA_DST_URI
	export SCYLLA_DST_URI := 10.254.254.200
endif
ifndef SCYLLA_IMAGE
	export SCYLLA_IMAGE := scylladb/scylla:6.0.2
endif

ifndef GOBIN
	export GOBIN := $(MAKEFILE_PATH)/bin
endif

define dl_bin
	@[ -d "$(GOBIN)" ] || mkdir -p "$(GOBIN)"; \
	if [ -L "$(GOBIN)/$(1)" ] && [ -e "$(GOBIN)/$(1)" ]; then \
		echo "$(GOBIN)/$(1) is already installed."; \
		return 0; \
	fi; \
	if $(GOBIN)/$(1) --version 2>/dev/null | grep "$(2)" >/dev/null; then \
		echo "$(GOBIN)/$(1) is already installed."; \
		return 0; \
	fi; \
	echo "$(GOBIN)/$(1) is not found, downloading."; \
	rm -f "$(GOBIN)/$(1)" >/dev/null 2>&1  \
	echo "Downloading $(GOBIN)/$(1)"; \
	curl --progress-bar -L $(3) --output "$(GOBIN)/$(1)"; \
	chmod +x "$(GOBIN)/$(1)";
endef

.PHONY: tune-aio-max-nr
tune-aio-max-nr:
	@bash -c '[[ "2097152" -ge "$(cat /proc/sys/fs/aio-max-nr)" ]] && sudo sh -c "echo 2097152 >> /proc/sys/fs/aio-max-nr"'

.PHONY: start-docker-environment
start-docker-environment: install-docker-compose tune-aio-max-nr
	$(GOBIN)/docker-compose -f ./ci/docker-compose.yml up -d
	until $(GOBIN)/docker-compose -f ./ci/docker-compose.yml exec -T source_node cqlsh -e "select * from system.local" ; do sleep 1; done
	until $(GOBIN)/docker-compose -f ./ci/docker-compose.yml exec -T destination_node cqlsh -e "select * from system.local" ; do sleep 1; done

.PHONY: stop-docker-environment
stop-docker-environment: install-docker-compose
	$(GOBIN)/docker-compose -f ./ci/docker-compose.yml kill

.PHONY: link-existing-docker-compose
link-existing-docker-compose:
	[ -d "$(GOBIN)" ] || mkdir -p "$(GOBIN)"
	whereis docker-compose | awk '{print $$2}' | xargs -I {} ln -s {} $(GOBIN)/docker-compose 2>/dev/null || true

.PHONY: install-docker-compose
install-docker-compose: DOCKER_COMPOSE_VERSION = 2.29.2
install-docker-compose: Makefile
ifeq ($(GOARCH),arm64)
	$(call dl_bin,docker-compose,${DOCKER_COMPOSE_VERSION},https://github.com/docker/compose/releases/download/v$(DOCKER_COMPOSE_VERSION)/docker-compose-$(GOOS)-aarch64)
else ifeq ($(GOARCH),amd64)
	$(call dl_bin,docker-compose,${DOCKER_COMPOSE_VERSION},https://github.com/docker/compose/releases/download/v$(DOCKER_COMPOSE_VERSION)/docker-compose-$(GOOS)-x86_64)
else
	@printf 'Unknown architecture "%s"\n', "$(GOARCH)" \
	@exit 69
endif

.PHONY: test
test: install-docker-compose start-docker-environment
	@echo "Running tests"
	@go test -v ./...

.PHONY: build
build:
	@echo "Building"
	@go build -v ./...

.PHONY: lint
lint:
	@go vet -v ./...

.PHONY: fix-lint
fix-lint:
	@go vet -v ./...

