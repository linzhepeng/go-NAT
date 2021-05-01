# 功能
- 可以实现通过任意电脑向家庭电脑上传文件
# 原理
![111](https://github.com/linzhepeng/go-NAt/blob/main/images/x.png)
# 特色  
- server与client通过心跳包保持连接。client断线自动重连
- 多协程并发上传下载
- 每个文件支持分块并发上传下载  
- 支持断点续传
- 实时显示上传进度
- server基本不需要关闭，如需重启可以只重启客户端。

# 说明
- upload：发起上传请求的主机
- server：具有公网ip地址的服务器
- client：内网主机

# 使用
- server文件夹的main.go文件编译成可执行文件运行在服务器上（消息通道默认监听443端口，数据传输通道默认监听80端口）
  
   `./server`
- client文件夹的main.go文件编译运行在内网主机上
  
    `./client -s 服务器ip -m 消息通道（默认443） -d 数据传输通道（默认80）`
- upload文件夹的main.go文件编译运行在要发起上传请求的主机上
  
    `./upload -s 服务器ip -p 服务器数据传输端口 -f 文件名 -c 上传并发数 -b分块大小`
