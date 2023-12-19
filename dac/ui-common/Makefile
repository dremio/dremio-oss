SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

.DEFAULT_GOAL := help

.PHONY: clean dependencies lint prepare test help

UI_LIB=../ui-lib

SRC = $(shell find src -type f)
LANG = $(shell find lang -type f)
SQL_GRAMMAR = $(shell find sql-grammar -type f)

clean: ## Clean all build and install artifacts
	@git clean -dfX

dependencies:
	@make --quiet --directory=$(UI_LIB) prepare

dist-cjs: node_modules tsconfig.json $(SRC) target/generated-sources/antlr
	pnpm exec swc ./src --config-file config/.swcrc-cjs --out-dir dist-cjs/src
	@touch dist-cjs

dist-esm: node_modules tsconfig.json $(SRC) target/generated-sources/antlr
	pnpm exec swc ./src --config-file config/.swcrc-esm --out-dir dist-esm/src
	pnpm exec swc ./target/generated-sources --config-file config/.swcrc-esm --out-dir dist-esm/target
	pnpm tsc --emitDeclarationOnly --declaration --declarationMap true --declarationDir dist-esm
	@touch dist-esm

dist-lang: node_modules $(LANG)
	pnpm exec ts-node ./scripts/build-lang.ts
	@touch dist-lang

lint: node_modules ## Lint JS
	pnpm exec eslint src --quiet

node_modules: pnpm-lock.yaml
	$(MAKE) dependencies
	pnpm install --frozen-lockfile --prefer-offline
	@touch node_modules

prepare: dependencies dist-esm dist-lang ## Build and package for distribution

target/generated-sources/antlr: node_modules $(SQL_GRAMMAR)
	pnpm exec antlr4ts -visitor -no-listener ./sql-grammar/*.g4 -o ./target/generated-sources/antlr -Xexact-output-dir
	@touch target/generated-sources/antlr

help: # Print a list of available commands (only commands with comments prefixed by ##)
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
