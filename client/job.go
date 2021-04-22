package main

import (
	"fmt"
	"github.com/jayi/golog"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"
)

const (
	RANGE_STATUS_INIT        = "init"        // 刚初始化, 未收到数据
	RANGE_STATUS_WRITING     = "writing"     // 已收到数据, 待发送
	RANGE_STATUS_DOWNLOADING = "downloading" // 已收到数据, 发送中
	RANGE_STATUS_DONE        = "done"        // 已发送数据
)

//文件分块信息
type RangeInfo struct {
	rangeStart int64
	rangeEnd   int64
	rangeSize  int64
	status     string
	data       []byte
}

//任务信息
type JobInfo struct {
	start      time.Time
	FileName   string       `json:"fileName"`
	FileSize   int64        `json:"fileSize"`
	PartLength int64        `json:"partLength"`
	Concurrent int64        `json:"concurrent"`
	RangeInfo  []*RangeInfo `json:"rangeInfo"`
	serverIP   string
	serverPort string
	fp         *os.File
	lock       sync.Mutex
}

//任务信息管理
type JobManager struct {
	jobs       map[string]*JobInfo
	jobChannel chan *JobInfo
	lock       sync.Mutex
	serverIP   string
	serverPort string
}

//在全局初始化任务管理，储存jobInfo
var jobManager = &JobManager{
	jobs:       make(map[string]*JobInfo),
	jobChannel: make(chan *JobInfo, 1000), // 任务管道, 当 message channel 收到任务时, 将任务写入该管道, 最多并发 1000 个任务
	lock:       sync.Mutex{},
}

//对jobManager成员赋值
func JobManagerInit(serverIP, serverPort string) {
	jobManager.serverIP = serverIP
	jobManager.serverPort = serverPort
	jobManager.run()
}

//初始化jobInfo
func (j *JobInfo) init(serverIP, serverPort string) error {
	j.serverIP = serverIP
	j.serverPort = serverPort
	j.RangeInfo = make([]*RangeInfo, 0)
	for start := int64(0); start < j.FileSize; start += j.PartLength {
		end := start + j.PartLength - 1
		if end > j.FileSize {
			end = j.FileSize - 1
		}
		j.RangeInfo = append(j.RangeInfo, &RangeInfo{
			rangeStart: start,
			rangeEnd:   end,
			rangeSize:  end - start + 1,
			status:     RANGE_STATUS_INIT,
		})
	}

	fp, err := os.Create(j.FileName)
	if err != nil {
		golog.Error(err)
		return err
	}
	j.fp = fp
	golog.Infof("job %s init success", j.FileName)
	return nil
}

func (m *JobManager) run() {
	//从通道中取出任务并执行
	for job := range m.jobChannel {
		golog.Infoln(job)
		//判断是否是旧任务，实现断点续传
		oldJob := m.findJob(job.FileName)
		if oldJob != nil {
			oldJob.reOpen()
			oldJob.doJob()
		} else {
			golog.Infof("receive new job %s %d", job.FileName, job.FileSize)
			//将任务信息注册到jobManager
			m.registerJob(job)
			job.init(m.serverIP, m.serverPort)
			job.doJob()
		}
	}
}

//从jobManager中找到jobInfo
func (m *JobManager) findJob(fileName string) (jobInfo *JobInfo) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.jobs[fileName]
}

//将jobInfo注册到jobManager
func (m *JobManager) registerJob(jobInfo *JobInfo) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.jobs[jobInfo.FileName] = jobInfo
}

//重新打开文件
func (j *JobInfo) reOpen() error {
	fp, err := os.OpenFile(j.FileName, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		golog.Error(err)
		return err
	}
	j.fp = fp
	golog.Infof("job %s reopen", j.FileName)
	return nil
}

//从服务器下载并写入磁盘
func (j *JobInfo) doJob() {
	//用channel做并发控制
	c := make(chan bool, j.Concurrent)
	wg := sync.WaitGroup{}
	for i := 0; i < len(j.RangeInfo); i++ {
		//判断是否断点续传
		if j.RangeInfo[i].status == RANGE_STATUS_DONE {
			continue
		}
		wg.Add(1)
		c <- true
		go func(i int) {
			defer wg.Done()
			url := fmt.Sprintf("http://%s:%s/get_file_part?fileName=%s&partNumber=%d", j.serverIP, j.serverPort, j.FileName, i+1)
			//从服务器分块下载文件
			if err := j.downloadPart(i, url); err != nil {
				return
			}
			//分块写入磁盘
			if err := j.writePart(i); err != nil {
				return
			}
			<-c
		}(i)
	}
	wg.Wait()
	j.done()
}

//下载
func (j *JobInfo) downloadPart(i int, url string) error {
	ri := j.RangeInfo[i]
	ri.status = RANGE_STATUS_DOWNLOADING

	//分块下载文件
	req, err := http.NewRequest(http.MethodGet, url, nil)
	client := http.Client{
		Timeout: 20 * time.Second,
	}

	var resp *http.Response
	//有可能从服务器读取响应失败，这里设置若失败则重新读取，最多读取3次
	for i := 0; i < 3; i++ {
		resp, err = client.Do(req)
		if err != nil {
			golog.Error(err)
			if i == 2 {
				return err
			}
			continue
		}
		break
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		golog.Error(err)
		return err
	}
	defer resp.Body.Close()

	// 用 http 状态码判断是否下载成功, server 需要显式设置状态码
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("download upload failed, error message %s", string(data))
		golog.Error(err)
		return err
	}
	ri.data = data
	golog.Infof("file %s download part %d:%d ok", j.FileName, i+1, len(j.RangeInfo))
	return nil
}

func (j *JobInfo) writePart(i int) error {
	ri := j.RangeInfo[i]
	ri.status = RANGE_STATUS_WRITING
	golog.Infof("file %s write part %d:%d:%d", j.FileName, i+1, len(j.RangeInfo), ri.rangeSize)
	//写文件不能并发，必须加锁
	j.lock.Lock()
	defer j.lock.Unlock()
	//定位偏移量
	_, err := j.fp.Seek(ri.rangeStart, 0)
	if err != nil {
		golog.Error(err)
		return err
	}
	//往文件中写数据
	_, err = j.fp.Write(ri.data)
	if err != nil {
		golog.Error(err)
		return err
	}
	ri.data = nil
	return nil
}

func (j *JobInfo) done() {
	golog.Infof("file %s download complete", j.FileName)
	j.fp.Close()
}
