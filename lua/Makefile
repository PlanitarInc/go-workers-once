.PHONY: default build
default: build
	
build: scripts.go

scripts.go: update_status.lua
	go-bindata -pkg lua -o $@ $^
	gofmt -w $@
