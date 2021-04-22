package main

import (
	"bytes"
	"encoding/json"
	"exercise/logger/fileLogger"
	"fmt"
	"github.com/jayi/golog"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	RANGE_STATUS_INIT    = "init"    //刚初始化，未收到数据
	RANGE_STATUS_READY   = "ready"   //已收到数据，待发送
	RANGE_STATUS_SENDING = "sending" //已收到数据，发送中
	RANGE_STATUS_DONE    = "done"    //发送完毕
)

//分块信息
type RangeInfo struct {
	RangeStart int64  `json:"rangeStart"`
	RangeEnd   int64  `json:"rangeEnd"`
	RangeSize  int64  `json:"rangeSize"`
	Status     string `json:"status"`
	data       []byte
}

//任务信息
type JobInfo struct {
	start      time.Time
	cost       time.Duration
	fp         *os.File
	FileName   string       `json:"fileName"`
	FileSize   int64        `json:"fileSize"`
	PartLength int64        `json:"partLength"` //每个分块的长度
	Concurrent int64        `json:"concurrent"` //并发数量
	RangeInfo  []*RangeInfo `json:"rangeInfo"`
	lock       sync.Mutex
}

var logger = fileLogger.NewLogger("info", "./", "log.log", "errLog.log", 10*1024*1024)

//JobInfo的初始化函数
func NewJob(filename string, partLength int64, concurrent int64) (*JobInfo, error) {
	fp, err := os.Open(filename)
	if err != nil {
		golog.Error(err)
		return nil, err
	}

	//读取文件的结构描述
	stat, err := fp.Stat()
	if err != nil {
		golog.Error(err)
		return nil, err
	}

	//生成任务，把打开的文件指针保存到JobInfo结构体中
	jobInfo := &JobInfo{
		start:      time.Now(),
		fp:         fp,
		FileName:   filename,
		FileSize:   stat.Size(),
		PartLength: partLength,
		Concurrent: concurrent,
	}

	//分配RangeInfo空间来存储分块文件
	jobInfo.RangeInfo = make([]*RangeInfo, 0)
	for start := int64(0); start < jobInfo.FileSize; start += partLength {
		end := start + jobInfo.PartLength - 1
		if end >= jobInfo.FileSize {
			end = jobInfo.FileSize - 1
		}
		jobInfo.RangeInfo = append(jobInfo.RangeInfo, &RangeInfo{
			RangeStart: start,
			RangeEnd:   end,
			RangeSize:  end - start + 1,
			data:       make([]byte, end-start+1),
			Status:     RANGE_STATUS_INIT,
		})
	}
	return jobInfo, nil
}

//初始化上传  通过http请求 发送任务信息给server
func (j *JobInfo) initUpload(ip, port string) error {
	url := fmt.Sprintf("http://%s:%s/job_init", ip, port)
	data, _ := json.Marshal(j)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		golog.Error(err)
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		golog.Error(err)
		return err
	}
	// 用 http 状态码判断是否初始化上传成功, server 需要显式设置状态码
	if resp.StatusCode != http.StatusOK {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			golog.Error(err)
			return err
		}
		defer resp.Body.Close()
		err = fmt.Errorf("init upload failed, error message %s", string(data))
		golog.Error(err)
	}
	//判断是否是之前的任务，是的话就把之前的任务信息赋值给j
	newData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		golog.Error(err)
		return err
	}
	if !strings.Contains(string(newData), "ok") {
		err = json.Unmarshal(newData, j)
		if err != nil {
			golog.Error(err, string(newData))
			return err
		}
	}
	logger.InfoF("init upload %s ok,", j.FileName)
	return nil
}

//分块读取本地文件
func (j *JobInfo) getRangeData(i int) error {
	j.lock.Lock()
	defer j.lock.Unlock()
	ri := j.RangeInfo[i]
	//定位分块文件的起始位置
	_, err := j.fp.Seek(ri.RangeStart, 0)
	if err != nil {
		golog.Error(err)
		return err
	}

	n, err := j.fp.Read(ri.data)
	if err != nil {
		golog.Error(err)
		return err
	}
	//判断读取的数据大小是否正确
	if n != len(ri.data) {
		err := fmt.Errorf("read data failed, start %d,expect %d length, read %d length", ri.RangeStart, len(ri.data), n)
		golog.Error(err)
		return err
	}

	ri.Status = RANGE_STATUS_READY
	return nil
}

//分块上传文件的函数
func (j *JobInfo) uploadPart(i int, url string) error {
	ri := j.RangeInfo[i]
	ri.Status = RANGE_STATUS_SENDING

	//上传data，把读到的data 放在HTTP body中
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(ri.data))
	if err != nil {
		golog.Error(err)
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		golog.Error(err)
		return err
	}

	if resp.StatusCode != http.StatusOK {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			golog.Error(err)
			return err
		}
		defer resp.Body.Close()
		err = fmt.Errorf("send message failed, err message %s", string(data))
		golog.Error(err)
		return err
	}

	logger.InfoF("send part %d of %s ok", i+1, j.FileName)
	ri.Status = RANGE_STATUS_DONE
	return nil
}

//上传的主要实现
func (j *JobInfo) run(ip, port string) {
	err := j.initUpload(ip, port)
	if err != nil {
		golog.Error(err)
		return
	}

	//用channel做并发控制
	c := make(chan bool, j.Concurrent)
	wg := sync.WaitGroup{}
	for i := 0; i < len(j.RangeInfo); i++ {
		//实现断点续传
		if j.RangeInfo[i].Status == RANGE_STATUS_DONE {
			continue
		}
		wg.Add(1)
		c <- true
		go func(i int) {
			defer wg.Done()
			//分块读取本地数据
			err := j.getRangeData(i)
			if err != nil {
				golog.Error(err)
				return
			}
			uploadUrl := fmt.Sprintf("http://%s:%s/upload_file_part?fileName=%s&partNumber=%d", ip, port, j.FileName, i+1)
			//分块上传数据到服务器
			err = j.uploadPart(i, uploadUrl)
			if err != nil {
				golog.Error(err)
				return
			}
			<-c
		}(i)
	}
	wg.Wait()
	j.done()
}

//关闭文件，计算耗时
func (j *JobInfo) done() {
	j.fp.Close()
	j.cost = time.Now().Sub(j.start)
}

func (j *JobInfo) progress() []*RangeInfo {
	return j.RangeInfo
}
