package main

import (
	"flag"
	"fmt"
	"github.com/jayi/golog"
	"os"
	"time"
)

// 打印任务进度, 非关键代码
func printProgress(job *JobInfo) {
	i := 0
	runningSymbol := []string{"-", "/", "\\"}
	for {
		fmt.Print("\r")
		progress := job.progress()
		fmt.Print("[ ")
		done := true
		for _, ri := range progress {
			symbol := ""
			switch ri.Status {
			case RANGE_STATUS_INIT:
				symbol = "□"
				done = false
			case RANGE_STATUS_READY:
				symbol = "▤"
				done = false
			case RANGE_STATUS_SENDING:
				symbol = "▤"
				done = false
			case RANGE_STATUS_DONE:
				symbol = "▣"
			}
			fmt.Print(symbol, " ")
		}
		fmt.Print("] ")
		i++
		fmt.Print(runningSymbol[i%len(runningSymbol)])
		if done && job.cost != 0 {
			os.Stdout.Sync()
			golog.Infof("上传成功, 耗时 %v s", job.cost.Seconds())
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func main() {
	var (
		serverIP   string
		serverPort string
		fileName   string
		concurrent int64
		partLength int64
	)

	flag.StringVar(&serverIP, "s", "175.27.189.134", "服务器IP地址（必填）")
	flag.StringVar(&serverPort, "p", "80", "服务器监听端口（默认80）")
	flag.StringVar(&fileName, "f", "", "上传文件名（必填）")
	flag.Int64Var(&concurrent, "c", 10, "上传并发数,默认10")
	flag.Int64Var(&partLength, "b", 1024*1024, "文件分块大小，默认1M")
	flag.Parse()

	if serverIP == "" || fileName == "" {
		flag.PrintDefaults()
		return
	}

	job, err := NewJob(fileName, partLength, concurrent)
	if err != nil {
		golog.Fatal(err)
	}

	go job.run(serverIP, serverPort)
	printProgress(job)
}
