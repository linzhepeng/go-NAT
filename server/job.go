package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/jayi/golog"
)

const (
	RANGE_STATUS_INIT    = "init"    // 刚初始化, 未收到数据
	RANGE_STATUS_READY   = "ready"   // 已收到数据, 待发送
	RANGE_STATUS_SENDING = "sending" // 已收到数据, 发送中
	RANGE_STATUS_DONE    = "done"    // 已发送数据
)

//分块数据
type RangeInfo struct {
	RangeStart int64  `json:"rangeStart"`
	RangeEnd   int64  `json:"rangeEnd"`
	RangeSize  int64  `json:"rangeSize"`
	Status     string `json:"status"`
	signal     chan bool
	data       []byte
}

//单个上传任务信息
type JobInfo struct {
	start      time.Time
	FileName   string       `json:"fileName"`
	FileSize   int64        `json:"fileSize"`
	PartLength int64        `json:"partLength"`
	Concurrent int64        `json:"concurrent"`
	RangeInfo  []*RangeInfo `json:"rangeInfo"`
}

//任务管理对象，用来储存jobInfo
type JobManager struct {
	jobs map[string]*JobInfo
	lock sync.Mutex
}

var jobManager = &JobManager{
	jobs: make(map[string]*JobInfo),
	lock: sync.Mutex{},
}

//任务初始化接口 用于处理上传端的initUpload请求
func jobInitHandler(w http.ResponseWriter, r *http.Request) {
	//读取上传端发来的http body
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		golog.Error(err)
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	j := new(JobInfo)
	err = json.Unmarshal(data, j)
	if err != nil {
		golog.Error(err)
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	//判断是否是之前的任务
	oldJob := jobManager.findJob(j.FileName)
	if oldJob != nil {
		respData, _ := json.Marshal(oldJob)
		w.Write(respData)
		if err := oldJob.runJob(); err != nil {
			golog.Error(err)
			w.Write([]byte(err.Error()))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		return
	}
	j.start = time.Now()

	//向client 发送任务信息，client 收到后向sever 请求分块数据，并分配好rangeInfo的空间
	err = j.runJob()
	if err != nil {
		golog.Error(err)
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// 将任务注册到任务管理对象中，后续通过唯一 filename 获取任务信息
	jobManager.registerJob(j)
	w.Write([]byte("init job " + j.FileName + "ok"))
}

//用于处理tool uploadPart 请求
func uploadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Write([]byte("err method"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	//从url读取参数
	fileName := r.URL.Query().Get("fileName")
	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if fileName == "" || partNumber <= 0 || err != nil {
		err := fmt.Errorf("upload param illegal, fileName %s, partNumber %s", fileName, r.URL.Query().Get("partNumber"))
		golog.Error(err)
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	//从jobManger中找到fileName对应的jobInfo
	jobInfo := jobManager.findJob(fileName)
	if jobInfo == nil {
		err := fmt.Errorf("not findJob job for fileName %s", fileName)
		golog.Error(err)
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	//读取tool发来的请求体
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		golog.Error(err)
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	//判断partNumber是否越界
	if partNumber > len(jobInfo.RangeInfo) {
		err := fmt.Errorf("illegal partNumber %d for fileName %s, max partNumber %d", partNumber, fileName, len(jobInfo.RangeInfo))
		golog.Error(err)
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ri := jobInfo.RangeInfo[partNumber-1]
	if ri.Status != RANGE_STATUS_INIT {
		golog.Infof("file %s part %d already received", jobInfo.FileName, partNumber)
		return
	}

	//判断数据大小是否正确
	if int64(len(data)) != ri.RangeSize {
		err := fmt.Errorf("illegal data size %d for partNumber %d fileName %s, expect data size %d", len(data), partNumber, fileName, ri.RangeSize)
		golog.Error(err)
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
	}

	//把请求体的数据写入
	ri.data = data

	golog.Infof("receive file %s part %d", jobInfo.FileName, partNumber)
	ri.Status = RANGE_STATUS_READY
	tryToSendSignal(ri.signal)
}

//用于处理client 拉取数据的请求
func sendHandler(w http.ResponseWriter, r *http.Request) {
	fileName := r.URL.Query().Get("fileName")
	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if fileName == "" || partNumber <= 0 || err != nil {
		err := fmt.Errorf("send param illegal, fileName %s, partNumber %s", fileName, r.URL.Query().Get("partNumber"))
		golog.Error(err)
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	jobInfo := jobManager.findJob(fileName)
	if jobInfo == nil {
		err := fmt.Errorf("not findJob job for fileName %s", fileName)
		golog.Error(err)
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if partNumber > len(jobInfo.RangeInfo) {
		err := fmt.Errorf("illegal partNumber %d for fileName %s, max partNumber %d", partNumber, fileName, len(jobInfo.RangeInfo))
		golog.Error(err)
		w.Write([]byte(err.Error()))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ri := jobInfo.RangeInfo[partNumber-1]

	//判断任务状态，可能上传还没有完成，用一个通道来阻塞
	if ri.Status == RANGE_STATUS_INIT {
		golog.Infof("waite file %s part %d", jobInfo.FileName, partNumber)
		<-ri.signal
	}
	golog.Infof("return file %s part %d", jobInfo.FileName, partNumber)
	ri.Status = RANGE_STATUS_SENDING
	w.Write(ri.data)
	ri.Status = RANGE_STATUS_DONE
	ri.data = nil
}

//初始化分块信息
func (j *JobInfo) runJob() error {
	if err := SendMessage(j.FileName, j.FileSize, j.PartLength, j.Concurrent); err != nil {
		golog.Error(err)
		return err
	}
	if j.RangeInfo != nil {
		return nil
	}
	j.RangeInfo = make([]*RangeInfo, 0)
	for start := int64(0); start < j.FileSize; start += j.PartLength {
		end := start + j.PartLength - 1
		if end >= j.FileSize {
			end = j.FileSize - 1
		}
		j.RangeInfo = append(j.RangeInfo, &RangeInfo{
			RangeStart: start,
			RangeEnd:   end,
			RangeSize:  end - start + 1,
			Status:     RANGE_STATUS_INIT,
			signal:     make(chan bool, 1),
			data:       make([]byte, 0),
		})
	}
	return nil
}

func (m JobManager) registerJob(jobInfo *JobInfo) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.jobs[jobInfo.FileName] = jobInfo
}

func (m *JobManager) findJob(fileName string) *JobInfo {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.jobs[fileName]
}

func tryToSendSignal(c chan bool) {
	select {
	case c <- true:
	default:
	}
}
