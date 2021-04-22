package main

import (
	"github.com/jayi/golog"
	"net/http"
)

func main() {
	http.HandleFunc("/job_init", jobInitHandler)
	http.HandleFunc("/upload_file_part", uploadHandler)
	http.HandleFunc("/get_file_part", sendHandler)

	err := Init("443")
	if err != nil {
		golog.Fatal(err)
	}

	go messageChannel.run()
	go messageChannel.heartbeat()

	http.ListenAndServe("0.0.0.0:80", nil)
}
