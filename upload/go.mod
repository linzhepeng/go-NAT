module Intranet_penetration/upload_tool

go 1.16

require github.com/jayi/golog v0.0.0-20190130074540-2c9fe7cdaba2

require (
	"exercise/logger/fileLogger" v0.0.0
)

replace (
	"exercise/logger/fileLogger"  => "../../exercise/logger/fileLogger"
)