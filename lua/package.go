// lua package embeds the lua scripts using github.com/jteeuwen/go-bindata
package lua

//go:generate go-bindata -pkg lua -o scripts.go update_status.lua
