package main

import (
	"encoding/binary"
	"fmt"
	"github.com/jayi/golog"
	"net"
	"time"
)

//心跳包
var HEARTBEAT_MESSAGE = []byte("123")

//消息通道，用来建立TCP连接，传输任务信息
type MessageChannel struct {
	serverIP        string
	serverPort      string
	conn            net.Conn
	reConnect       chan bool
	buffer          []byte      //用来储存server发来的信息
	dataToWrite     chan []byte //储存client要发送的信息
	cacheData       []byte      //处理粘包的时候会用到
	maxCacheDataLen int         //同上
	cacheDataLen    int         //同上
}

//初始化全局变量
var messageChannel = &MessageChannel{
	conn:            nil,
	reConnect:       make(chan bool),
	buffer:          make([]byte, 2048),
	dataToWrite:     make(chan []byte, 1),
	cacheData:       make([]byte, 0),
	maxCacheDataLen: 0,
	cacheDataLen:    0,
}

//messageChannel 的一些初始化赋值
func messageChannelInit(serverIP, serverPort string) {
	messageChannel.init(serverIP, serverPort)
}

func (m *MessageChannel) init(serverIP, serverPort string) {
	messageChannel.serverIP = serverIP
	messageChannel.serverPort = serverPort
	//第一个发送的数据是心跳包
	go m.run()
}

//连接server
func (m *MessageChannel) run() {
	go m.connect()
	go m.read()
	go m.write()
}

func (m *MessageChannel) connect() {
	for {
		conn, err := net.Dial("tcp", m.serverIP+":"+m.serverPort)
		if err != nil {
			golog.Error(err)
			//连接失败每秒尝试一次
			time.Sleep(time.Second)
			continue
		}
		m.conn = conn
		m.dataToWrite <- HEARTBEAT_MESSAGE
		//这里用一个通道阻塞住，若连接断开，通道会写入数据解除阻塞，重新连接
		<-m.reConnect
	}
}

func (m *MessageChannel) read() {
	for {
		if m.conn == nil {
			//连接未建立则100ms后重试
			time.Sleep(time.Millisecond * 100)
			continue
		}
		_ = m.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		//读取server发来的数据
		n, err := m.conn.Read(m.buffer)
		if err != nil {
			golog.Error(err)
			m.doReConnect()
			continue
		}
		//判断是否是任务信息
		err = m.getCompleteData(m.buffer[:n], n)
		if err != nil {
			golog.Error(err)
		}
	}
}

//实现重连
func (m *MessageChannel) doReConnect() {
	m.conn = nil
	m.reConnect <- true
}

//判断是否是心跳包
func (m *MessageChannel) isHeartbeat(data []byte) bool {
	return string(data) == "123"
}

//从缓冲区中读取server发来的消息，这里有可能会粘包（心跳包和任务信息一起发过来，或者任务消息一次没发全）
func (m *MessageChannel) getCompleteData(data []byte, n int) error {
	//递归的退出条件
	if n <= 0 {
		return nil
	}

	if data == nil {
		return nil
	}
	golog.Infoln("receive data ", data, n)

	// 当前已读的数据长度
	currentReadLen := 0
	if m.maxCacheDataLen == 0 {
		err := fmt.Errorf("illegal job data for %v", data)
		if len(data) < 4 {
			return err
		}
		//大端读取前四个字节
		m.maxCacheDataLen = int(binary.BigEndian.Uint32(data[:4]))
		//重置缓存区数据
		m.cacheData = make([]byte, m.maxCacheDataLen)
		data = data[4:n]
		//记录已读长度
		currentReadLen += 4
	}
	//这个包应该要读的长度
	restLen := m.maxCacheDataLen - m.cacheDataLen
	//如果data长度小于要读的长度，说明发生了粘包，有一部分数据没有在这一次传输过来
	if len(data) < restLen {
		restLen = len(data)
	}
	currentReadLen += restLen
	//把读到的数据拷贝到缓存区
	copy(m.cacheData[m.cacheDataLen:], data[:restLen])
	m.cacheDataLen += restLen
	//判断是否读完应该要读的长度
	if m.cacheDataLen < m.maxCacheDataLen {
		return nil
	}
	//判断是心跳包还是任务信息
	job, err := m.processData(m.cacheData, m.maxCacheDataLen)
	if err != nil {
		golog.Error(err)
	}
	//将任务发送到jobManager
	if job != nil {
		registerNewJob(job)
	}
	//走到这说明已经完整的读取了一个包，这里重置缓存长度，继续读取之后的数据
	m.maxCacheDataLen = 0
	m.cacheDataLen = 0
	return m.getCompleteData(data[restLen:], n-currentReadLen)
}

func (m *MessageChannel) processData(data []byte, dataLen int) (*JobInfo, error) {
	//如果是心跳包，则将心跳包重新写入 dataToWrite ，以便write协程可以继续发送
	if m.isHeartbeat(data[:dataLen]) {
		m.dataToWrite <- HEARTBEAT_MESSAGE
		return nil, nil
	}

	return m.newJob(data[:dataLen])
}

//根据server发来的消息初始化jobInfo，用大端读取
func (m *MessageChannel) newJob(data []byte) (*JobInfo, error) {
	golog.Infoln("new job for ", data)
	err := fmt.Errorf("illegal job data for %v", data)
	if len(data) < 4 {
		golog.Errorln(data, err)
		return nil, err
	}
	fileNameLen := int(binary.BigEndian.Uint32(data[:4]))
	data = data[4:]
	if fileNameLen <= 0 || len(data) < 4 {
		golog.Errorln(data, err)
		return nil, err
	}
	fileName := data[:fileNameLen]
	data = data[fileNameLen:]

	if len(data) < 8 {
		golog.Errorln(data, err)
		return nil, err
	}
	fileSize := int64(binary.BigEndian.Uint64(data[:8]))
	data = data[8:]

	if len(data) < 4 {
		golog.Errorln(data, err)
		return nil, err
	}
	partLength := int(binary.BigEndian.Uint32(data[:4]))
	data = data[4:]

	if len(data) < 4 {
		golog.Errorln(data, err)
		return nil, err
	}
	concurrent := int(binary.BigEndian.Uint32(data[:4]))

	//回复server端，表示已收到任务信息
	m.dataToWrite <- fileName
	return &JobInfo{
		start:      time.Now(),
		FileName:   string(fileName),
		FileSize:   fileSize,
		PartLength: int64(partLength),
		Concurrent: int64(concurrent),
	}, nil
}

//写入磁盘
func (m *MessageChannel) write() {
	for {
		//从待发送channel中拿出数据，直接发送
		data := <-m.dataToWrite

		//连接未建立则100ms后尝试
		if m.conn == nil {
			time.Sleep(time.Millisecond * 100)
			continue
		}
		_ = m.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_, err := m.conn.Write(data)
		if err != nil {
			golog.Error(err)
			m.doReConnect()
			continue
		}
		golog.Infof("send heartbeat %s successful", string(data))
		time.Sleep(3 * time.Second)
	}
}

//将任务信息发到任务通道中
func registerNewJob(job *JobInfo) {
	jobManager.jobChannel <- job
}
