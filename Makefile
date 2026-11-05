.PHONY: default build lint-claude-md
default: build

build:
	go generate ./...

lint-claude-md:
	bash script/lint-claude-md.sh
