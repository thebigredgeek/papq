PATH := node_modules/.bin:$(PATH)
SHELL := /bin/bash

NODE ?= $(shell which node)
YARN ?= $(shell which yarn)
PKG ?= $(if $(YARN),$(YARN),$(NODE) $(shell which npm))

.PHONY:

all: clean .PHONY
	tsc

configure: .PHONY
	@NODE_ENV= $(PKG) install

clean: .PHONY
	rimraf dist

test: .PHONY
	node chaos-monkey-test.js
	mocha -r ts-node/register test/spec.ts	

