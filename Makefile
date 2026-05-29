IMAGE     ?= kage
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
REGISTRY  ?= ## e.g. 123456789.dkr.ecr.us-east-1.amazonaws.com
PLATFORMS ?= linux/amd64,linux/arm64

GO        := go
BINARY    := kaged
BUILD_DIR := ./bin

.PHONY: all build test lint docker docker-push clean

## ── Local development ─────────────────────────────────────────────────────────

all: build

build:
	$(GO) build -trimpath -ldflags="-s -w" -o $(BUILD_DIR)/$(BINARY) ./cmd/kaged

test:
	$(GO) test -race -count=1 ./...

lint:
	$(GO) vet ./...

## ── Docker ────────────────────────────────────────────────────────────────────

# Build a single-platform image for the local Docker daemon (fast iteration).
docker:
	docker build \
		--build-arg VERSION=$(VERSION) \
		-t $(IMAGE):$(VERSION) \
		-t $(IMAGE):latest \
		.

# Build and push a multi-platform manifest (amd64 + arm64) to the registry.
# Requires: docker buildx with a builder that supports cross-compilation.
docker-push:
	docker buildx build \
		--platform $(PLATFORMS) \
		--build-arg VERSION=$(VERSION) \
		-t $(REGISTRY)/$(IMAGE):$(VERSION) \
		-t $(REGISTRY)/$(IMAGE):latest \
		--push \
		.

clean:
	rm -rf $(BUILD_DIR)
