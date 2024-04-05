SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

.DEFAULT_GOAL := help

.PHONY: clean prepare help

SRC = $(shell find src -type f)

clean: ## Clean all build and install artifacts
	@git clean -dfX

dist: node_modules $(SRC)
	pnpm run dist
	@touch dist

node_modules: pnpm-lock.yaml
	pnpm install --frozen-lockfile --prefer-offline

prepare: dist ## Build and package for distribution

help: # Print a list of available commands (only commands with comments prefixed by ##)
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
