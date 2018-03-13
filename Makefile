.PHONY: default build
default: build

build:
	go generate ./...
