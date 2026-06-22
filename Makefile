# Multiscan Makefile — build, install, release
.PHONY: all build build-all build-linux-amd64 build-linux-arm64 \
        install install-master install-worker uninstall \
        release clean

VERSION ?= $(shell git describe --tags --always 2>/dev/null || echo "dev")
COMMIT  ?= $(shell git log -1 --format=%h 2>/dev/null || echo "unknown")
DATE    ?= $(shell date -u +%Y%m%d-%H%M%S)
LDFLAGS := -ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE) -s -w"

BIN_DIR  := bin
RELEASE_DIR := release

# ── Default: build for current platform ───────────────────────────────
all: build

build:
	@echo "Building for $(shell go env GOOS)/$(shell go env GOARCH)..."
	go build $(LDFLAGS) -o $(BIN_DIR)/master ./cmd/master
	go build $(LDFLAGS) -o $(BIN_DIR)/worker ./cmd/worker
	@echo "→ $(BIN_DIR)/master  $(BIN_DIR)/worker"

# ── Cross-platform builds ─────────────────────────────────────────────
build-linux-amd64:
	@echo "Building linux/amd64..."
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BIN_DIR)/master-linux-amd64 ./cmd/master
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BIN_DIR)/worker-linux-amd64 ./cmd/worker
	@echo "→ $(BIN_DIR)/master-linux-amd64  $(BIN_DIR)/worker-linux-amd64"

build-linux-arm64:
	@echo "Building linux/arm64..."
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(BIN_DIR)/master-linux-arm64 ./cmd/master
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(BIN_DIR)/worker-linux-arm64 ./cmd/worker
	@echo "→ $(BIN_DIR)/master-linux-arm64  $(BIN_DIR)/worker-linux-arm64"

build-all: build-linux-amd64 build-linux-arm64

# ── Install ────────────────────────────────────────────────────────────
install: install-master install-worker install-cli

install-master: build
	cp $(BIN_DIR)/master /usr/local/bin/multiscan-master
	@echo "Installed master binary"

install-worker: build
	cp $(BIN_DIR)/worker /usr/local/bin/multiscan-worker
	@echo "Installed worker binary"

install-cli:
	cp scripts/multiscan.sh /usr/local/bin/multiscan
	chmod +x /usr/local/bin/multiscan
	@echo "Installed 'multiscan' CLI command"

# ── Systemd setup ──────────────────────────────────────────────────────
install-systemd: build-all
	@bash install.sh all

# ── One-click (systemd + everything) ────────────────────────────────────
oneclick: build-all
	@bash install.sh all

uninstall:
	@bash scripts/uninstall.sh

# ── Release tarballs ──────────────────────────────────────────────────
release: build-all
	@mkdir -p $(RELEASE_DIR)
	@for plat in linux-amd64 linux-arm64; do \
		name="multiscan-$$plat-$(VERSION)"; \
		mkdir -p /tmp/$$name; \
		cp $(BIN_DIR)/master-$$plat /tmp/$$name/master; \
		cp $(BIN_DIR)/worker-$$plat /tmp/$$name/worker; \
		cp install.sh /tmp/$$name/ 2>/dev/null || true; \
		cp scripts/uninstall.sh /tmp/$$name/ 2>/dev/null || true; \
		cp scripts/multiscan.sh /tmp/$$name/ 2>/dev/null || true; \
		cp README.md /tmp/$$name/ 2>/dev/null || true; \
		cd /tmp && tar czf $(CURDIR)/$(RELEASE_DIR)/$$name.tar.gz $$name; \
		rm -rf /tmp/$$name; \
		echo "→ $(RELEASE_DIR)/$$name.tar.gz"; \
	done

# ── Clean ──────────────────────────────────────────────────────────────
clean:
	rm -rf $(BIN_DIR)/* $(RELEASE_DIR)/
	@echo "cleaned"
