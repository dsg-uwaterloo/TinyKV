# 流程

## 角色说明
- 客户端：这里的添加、删除节点，都是客户端发起的。
- tinykvServer：具体处理这些请求、进行raft共识的服务器。这里有可以分为几个小部分
  - ts-raft: raft算法具体实现。
  - ts-SchedulerTaskHandler：这里会定时触发与scheeduler的心跳消息SchedulerRegionHeartbeatTask.
  - ts-peerMsgHandler: 
    - 这个是专门处理各种raft消息的handler，暂时处理过的有：
      - `admin`: 修改raft集群配置的（暂时有tranferLeader，confChange）
      - `cmd`: 客户端发过来的，比如setkv，getkv等等
      - `raft`： 正常raft消息，用于ts-raft（算法）处理消息。
        - 现在情况来看，admin、cmd一般都会转为raft消息，触发raft执行操作或者达成共识。
- schedulerClient：这里主要是mok的client，真实的情况估计是这样的


# 注意点
## transfer leader
- 小优化：可以选择一个log最接近到follower，来tranfer.
- `单节点成员变更`：一次只能变更一个成员（添加或删除）
  - 一次：指都是raft消息收入到apply这个confChange，是完整到一次。
  - 只能变更一个：这更一次中，只能有一个confChange消息。如果有多个，那么就直接drop。
- 如果日志不同步，那么就需要先同步日志
  - 最接近的follower，日志相差不可能有两个confChange。
    - leader是不会允许有两个confChange同时进行的。
    - 最接近的follower有两个ConfChange，那么这个confChange不可能commit（因为需要大多数，但是最近的都没有这个confChange，其它更不可能有来，所以不可能commit）——跟leader不允许有两个ConfChange同时进行矛盾.


# region-split
kv/raftstore/runner/split_check.go  ---> MsgSplitRegion   ---> onPrepareSplitRegion(ids)  --->  onAskBatchSplit(kv/raftstore/runner/pd_task.go)

## 
- 1、splitMsg(admin) 
- 2、raft 共识到每个节点（后续都是每个节点执行） 
- 3、执行region-split 
  - 3.1、split本质上来说，仅仅说region进行分割，kv数据是不需要到
    - 因为region不管怎么变，但是store是没有变到，所以数据存储也不需要变
    - region变化好来之后，直接save起来，包括`ctx.storeMeta.regionRanges`,`router.regions`,`ctx.storeMeta.regions`,`$cache`, `adn so on...`


//可能之前是从Candidate过来的，所以这里先要置空,否则可能出现多leader情况.
	r.votes = nil

ReqTerm     uint64 `json:"req_term"`    //请求消息对应都term,leader处理响应消息都时候，会判断下是否是最新都term请求都voteReq；否则直接丢弃.


#
- 主要，服务器开发（C++/GO），写过半年js；偶尔会用shell等写个小工具。
- 主要擅长服务器开发，微服务架构。
- 最近两年主要是做区块链，对分布式算法有研究，主要是raft、pbft研究相对更深入点。
- 做过部署工具，一开始是纯进程的，后来慢慢升级为docker版本的。对docker、k8s有过了解。



















