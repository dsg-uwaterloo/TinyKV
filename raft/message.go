package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

type message interface {
	toPbMsg() pb.Message
}

type ReqVote struct {
	Term         uint64 `json:"term"`
	CandidateId  uint64 `json:"cid"`
	LastLogIndex uint64 `json:"ll_idx"`
	LastLogTerm  uint64 `json:"ll_term"`
}

func (rv *ReqVote) toPbMsg() pb.Message {
	return pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		Term:    rv.Term,
		Index:   rv.LastLogIndex,
		LogTerm: rv.LastLogTerm,
	}
}

func (rv *ReqVote) fromPbMsg(m pb.Message) {
	rv.Term = m.GetTerm()
	rv.CandidateId = m.GetFrom()
	rv.LastLogTerm = m.GetLogTerm()
	rv.LastLogIndex = m.GetIndex()
}

type RspVote struct {
	Term        uint64 `json:"term"`        //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool   `json:"voteGranted"` //候选人赢得了此张选票时为真
	//ReqTerm     uint64 `json:"req_term"`    //NOTICE-raft:请求消息对应都term,leader处理响应消息都时候，会判断下是否是最新都term请求都voteReq；否则直接丢弃.
}

func (rv *RspVote) toPbMsg() pb.Message {
	return pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		Term:    rv.Term,
		Reject:  !rv.VoteGranted,
		//LogTerm: rv.ReqTerm,
	}
}

func (rv *RspVote) fromPbMsg(m pb.Message) {
	rv.Term = m.GetTerm()
	rv.VoteGranted = !m.GetReject()
	//rv.ReqTerm = m.GetLogTerm()
}

type ReqHeartbeat struct {
	Term           uint64 `json:"term"`
	LeaderId       uint64 `json:"lid"`
	PrevLogIndex   uint64 `json:"pl_index"`
	PrevLogTerm    uint64 `json:"pl_term"`
	LeaderCommitId uint64 `json:"l_commit_id"`
}

func (rh *ReqHeartbeat) toPbMsg() pb.Message {
	return pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term:    rh.Term,
		LogTerm: rh.PrevLogTerm,
		Index:   rh.PrevLogIndex,
		Commit:  rh.LeaderCommitId,
	}
}
func (rh *ReqHeartbeat) fromPbMsg(m pb.Message) {
	rh.Term = m.GetTerm()
	rh.LeaderId = m.GetFrom()
	rh.PrevLogIndex = m.GetIndex()
	rh.PrevLogTerm = m.GetLogTerm()
	rh.LeaderCommitId = m.GetCommit()
}

type RspHeartbeat struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"succ"`
	//NOTICE-raft:如果失败（RspHeartbeat.Sucess==false），那么就填充req相关数据；如果成功，则忽略该值;
	//reqPrevLogIndex uint64 `json:"req_prev_log_index"`
	//reqPrevLogTerm  uint64 `json:"req_prev_log_term"`
}

func (rv *RspHeartbeat) toPbMsg() pb.Message {
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		Term:    rv.Term,
		Reject:  !rv.Success,
	}
	//if false == rv.Success {
	//	m.Index = rv.reqPrevLogIndex
	//	m.LogTerm = rv.reqPrevLogTerm
	//}
	return m
}

func (rv *RspHeartbeat) fromPbMsg(m pb.Message) {
	rv.Term = m.GetTerm()
	rv.Success = !m.GetReject()
	//
	//if false == rv.Success {
	//	rv.reqPrevLogIndex = m.GetIndex()
	//	rv.reqPrevLogTerm = m.GetLogTerm()
	//}
}

type ReqAppend struct {
	ReqHeartbeat
	Entries []*pb.Entry `json:"entries"`
}

func (ra *ReqAppend) copyEntries(ents []pb.Entry) {
	hasConfChange := false
	for _, e := range ents {
		tmp := new(pb.Entry)
		if e.EntryType == pb.EntryType_EntryConfChange {
			//一次最多传一个ConfChange消息.
			if hasConfChange {
				return
			} else {
				hasConfChange = true
			}
		}
		*tmp = e
		ra.Entries = append(ra.Entries, tmp)
	}
}

func (ra *ReqAppend) toPbMsg() pb.Message {
	m := ra.ReqHeartbeat.toPbMsg()
	m.MsgType = pb.MessageType_MsgAppend
	m.Entries = ra.Entries
	return m
}
func (ra *ReqAppend) fromPbMsg(m pb.Message) {
	ra.ReqHeartbeat.fromPbMsg(m)
	ra.Entries = m.GetEntries()
}

type RspAppend struct {
	RspHeartbeat
	LastLogIndex uint64 `json:"last_log_idx"`
}

func (ra *RspAppend) toPbMsg() pb.Message {
	m := ra.RspHeartbeat.toPbMsg()
	m.MsgType = pb.MessageType_MsgAppendResponse
	//NOTICE-raft:如果失败（RspHeartbeat.Sucess==false），那么就填充req相关数据；如果成功，则忽略该值;
	if ra.Success {
		m.Index = ra.LastLogIndex
	} else {
		//NOTICE-raft:可能会存在情况：系统网络速度比较慢（比心跳都慢），所以这里就导致相同都消息多次发送，导致多次返回失败；
		//	多次返回失败，导致leader的Progress多次减少，导致snapshot.
		//  由于心跳也有这个问题，所以这个赋值放到心跳消息中做.
		//m.Index = ra.reqPrevLogIndex
		//m.LogTerm = ra.reqPrevLogTerm
	}
	return m
}
func (ra *RspAppend) fromPbMsg(m pb.Message) {
	ra.RspHeartbeat.fromPbMsg(m)
	if ra.Success {
		ra.LastLogIndex = m.GetIndex()
	}
}

//term	领导人的任期号
//leaderId	领导人的 Id，以便于跟随者重定向请求
//lastIncludedIndex	快照中包含的最后日志条目的索引值
//lastIncludedTerm	快照中包含的最后日志条目的任期号
//offset	分块在快照中的字节偏移量
//data[]	从偏移量开始的快照分块的原始字节
//done	如果这是最后一个分块则为 true
type ReqSnapshot struct {
	Term     uint64
	LeaderId uint64
	//snapshot;
	LastIndex uint64
	LastTerm  uint64
	//snapshot data split n blocks;do nothing  here
	Offset uint64
	Data   []byte
	Done   bool
}

func (r *ReqSnapshot) fromPbMsg() {

}
