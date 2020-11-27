```shell
kv/
├── raftstore/
|   ├── bootstrap.go
|   ├── bootstrap_test.go
|   ├── cmd_resp.go
|   ├── node.go
|   ├── peer.go
|   ├── peer_msg_handler.go
|   ├── peer_storage.go
|   ├── peer_storage_test.go
|   ├── raft_worker.go
|   ├── raftstore.go
|   ├── router.go
|   ├── store_worker.go
|   ├── ticker.go
|   |   -------------------------
│   ├── message/
│   ├── meta/
│   ├── runner/
│   ├── scheduler_client/
│   ├── snap/
│   └── util/
└── storage/
    ├── raft_storage/
    |   ├── raft_client.go
    |   ├── raft_server.go
    |   ├── region_reader.go
    |   ├── resolver.go
    |   ├── snap_runner.go
    |   └── transport.go
    |
    └── standalone_storage/
```

# 一、storage/raft_storage
&emsp;&emsp;`storage目录`下的`storage接口`一种实例化，类比于standalone_storage.  
&emsp;&emsp;该`storage接口`对server提供服务支持 
 - 1- RaftClient/RaftStorage
 - 2- RegionReader
 - 3- resolverRunner
 - 4- ServerTransport : `raftStore` 下的 `Transport 接口`
 - 5- snapRunner 

# 二、raftstore/
## 1-scheduler_client/client.go
- pd(placement drive)的一个简单实现；具体可以看`scheduler/client`下的`Client接口`声明.
- pd是用于副本管理、调度的一个服务器；具体可以看pingcap的blog.
- `scheduler_client/client.go`对象，用于向pd上报当前store的情况，并且接受pd的调度消息。
- resolver.go 主要配置client，用于解析scheduler服务器的地址.

## 2-peer_storage.go(PeerStorage):用于实现`raft算法`的`storage接口`
- raft算法本身需要一个storage的存储对象提供存储服务
- 可以类比于`raft/storage.go`下的`MemoryStorage结构体`
  
## 3-peer.go 下peer 类.
- 主要对`raft下的 RawNode` 进行封装
- 可以理解为raft算法中的一个节点
- peer对象下，会有一个`PeerStorage对象`，给`RawNode`提供存储服务.
- peer对象下，会有一个`tick`提供定时器功能——在raft算法中，会有tick运行（election/heartbeat定时器）
  
## 4-node.go 下 Node 类
- raftGroup中的一个节点（这里是网络上的概念）
- raft算法中的节点，是个逻辑概念，这里有peer封装.
- router.go 主要对 消息进行分发
- 是不是raft消息？

## store_worker.go 
   该文件，主要负责跟你pdserver定期同步数据.  
   理论上来说，这里的raft主要是面向raft算法的；store主要面向pdserver的（用于定期更新pd上的store信息）.

## raft_worker.go 
   raft 系统的reciver消息,处理raft的业务.
## peer_handle_msg.go (peerMsgHandler)
   实际处理raft消息的地方 .

## snap/ 目录
   主要是snap从创建到传输完成（raft系统之间），管理整个过程。简单看看状态维护，大约有个数.
```golang
func (e SnapEntry) String() string {
	switch e {
	case SnapEntryGenerating:
		return "generating"
	case SnapEntrySending:
		return "sending"
	case SnapEntryReceiving:
		return "receiving"
	case SnapEntryApplying:
		return "applying"
	}
	return "unknown"
}
```

# 三、其它
## 1- util/worker/
   worker可以理解为线程池，主要用于处理task
- start启动一个goroutine运行loop
  - sender用于往worker发送task.
  - reciver接收task(sener传进来的)，运行task.
  
```golang
func NewWorker(name string, wg *sync.WaitGroup) *Worker {
	ch := make(chan Task, defaultWorkerCapacity)
	return &Worker{
		sender:   (chan<- Task)(ch),  //sender用于往worker发送task.
		receiver: (<-chan Task)(ch),  //reciver接收task(sener传进来的)，运行task.
		name:     name,
		wg:       wg,                 //为了等待所有goroutine退出.
	}
}
```


# snapshot
- leader
  - storage(memeory) can createSnapshot
  - rawnode can call step for msgSnap（compact the raftlog entries)
  - storage.ApplySnapshot can called at app
  - when follower handle on `AppendEntry` and prevLogIndex compare failed(mybe too slow,mybe new node),leader send snapshot
- follower
  - rawnode call step for msgSnap (compact the raftlog entries)
  - app compact the storage logs
- rawnode `ready` has snapshot ,can called for the app,and applySnapshot for storage.



# snapshot2
- 每一个raftNode都会定时/定量创建snapshot
- `send` : 触发snapshot消息
  - 新节点加入到已经存在raft系统中
  - 落后很多消息多follower节点——可能是因为网络太差，导致落后，而leader中多entry已经compact了，没有日志了，索引只能snapshot消息了.
- `recive` : 处理snapshot消息
  - 1、如果term < currentTerm就立即回复（同样多配方，同样多处理）
  - 2、保存快照文件，丢弃具有较小索引的任何现有或部分快照
    - 较小，索引 —— follower多raftlog多index和snapshot中多index进行比较

```shell
# key{regionId:1,term:2,index:3},cfs{default,write,lock}
# prefix= 'gen_1_2_3'
# '$prefix'_'$cf'.sst
# '$prefix'_'$cf'.sst.tmp
gen_1_2_3_default.sst.tmp  --->     gen_1_2_3_default.sst
gen_1_2_3_write.sst.tmp    --->     gen_1_2_3_write.sst
gen_1_2_3_lock.sst.tmp     --->     gen_1_2_3_lock.sst

gen_1_2_3.meta.tmp         --->     gen_1_2_3.meta

```

### eveny node
storage.Snapshot ---> buildSnapshot
                              |
                              V
                           regionTask ---> doGenSnapshot
### leader node
raftMsg(snapshot) ---> snapRunner.sendSnapTask

### follower node
raft_server.Snapshot 
               |
               V
            snapRunner.recvSnap  ---> raftMsg(snapshot) `router` 
                                             |
                                             V
                                          peerMsgHandler.OnRaftMsg  ---> raftNode.Step(snapshot)

1、触发生成snapshot：我看代码是这定时器gc触发CompactLogRequest，然后走raft同步，同时触发snapshot组文件构建
2、落后的follower安装snapshot：在leader发现follower节点需要触发


















