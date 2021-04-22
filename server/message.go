package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/jayi/golog"
	"net"
	"time"
)

type MessageChannel struct {
	port     string
	listener net.Listener
	conn     net.Conn
	reAccept chan bool
}

var messageChannel = &MessageChannel{
	listener: nil,
	conn:     nil,
	reAccept: make(chan bool),
}

//初始化messageChannel
func (m *MessageChannel) init(port string) error {
	messageChannel.port = port
	l, err := net.Listen("tcp", ":"+messageChannel.port)
	if err != nil {
		golog.Error(err)
		return err
	}

	messageChannel.listener = l
	return nil
}

//对init函数进行封装
func Init(port string) error {
	return messageChannel.init(port)
}

//接收连接
func (m *MessageChannel) run() {
	for {
		conn, err := m.listener.Accept()
		if err != nil {
			golog.Error(err)
			return
		}
		golog.Info("receive new connection")
		m.conn = conn
		<-m.reAccept
	}
}

//心跳包
func (m *MessageChannel) heartbeat() {
	for {
		if m.conn == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		m.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 2048)
		n, err := m.conn.Read(buf)
		golog.Infoln("read from client:", n, string(buf[:n]))
		if err != nil {
			golog.Error("read heartbeat failed")
			m.conn.Close()
			m.conn = nil
			m.reAccept <- true
			continue
		}

		uint32Slice := make([]byte, 4)
		binary.BigEndian.PutUint32(uint32Slice, 3)
		uint32Slice = append(uint32Slice, []byte("123")...)
		_, err = m.conn.Write(uint32Slice)
		if err != nil {
			golog.Error("send heartbeat failed")
			m.conn.Close()
			m.conn = nil
			m.reAccept <- true
		}
	}
}

//大端写入，为了解决粘包，前四个字节先写入数据的总长度
func (m *MessageChannel) sendMessage(fileName string, fileSize int64, partLength int64, concurrent int64) error {
	if m.conn == nil {
		err := errors.New("message channel is not connected")
		golog.Error(err)
		return err
	}

	buff := bytes.Buffer{}
	uint32Slice := make([]byte, 4)
	binary.BigEndian.PutUint32(uint32Slice, uint32(len(fileName)))
	buff.Write(uint32Slice)
	buff.Write([]byte(fileName))

	uint64Slice := make([]byte, 8)
	binary.BigEndian.PutUint64(uint64Slice, uint64(fileSize))
	buff.Write(uint64Slice)
	binary.BigEndian.PutUint32(uint32Slice, uint32(partLength))
	buff.Write(uint32Slice)
	binary.BigEndian.PutUint32(uint32Slice, uint32(concurrent))
	buff.Write(uint32Slice)

	binary.BigEndian.PutUint32(uint32Slice, uint32(len(buff.Bytes())))
	_, err := m.conn.Write(uint32Slice)
	if err != nil {
		golog.Error(err)
		return err
	}

	_, err = m.conn.Write(buff.Bytes())
	if err != nil {
		golog.Error(err)
		return err
	}
	golog.Infof("send message: file name: %s, file size: %d, part length: %d, concurrent: %d\n", fileName, fileSize, partLength, concurrent)
	return nil
}

func SendMessage(fileName string, fileSize int64, partLength int64, concurrent int64) error {
	return messageChannel.sendMessage(fileName, fileSize, partLength, concurrent)
}
