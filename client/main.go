package main

import (
	"flag"
)

func main() {
	var (
		serverIP           string
		messageChannelPort string
		dataPort           string
	)
	flag.StringVar(&serverIP, "s", "", "服务器IP（必填）")
	flag.StringVar(&messageChannelPort, "m", "443", "服务器监听端口（消息通道），默认443")
	flag.StringVar(&dataPort, "d", "80", "服务器监听端口（数据传输），默认80")
	flag.Parse()

	if serverIP == "" {
		flag.PrintDefaults()
		return
	}

	messageChannelInit(serverIP, messageChannelPort)
	JobManagerInit(serverIP, dataPort)
}

// test_branch
