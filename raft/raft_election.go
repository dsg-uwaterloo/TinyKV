package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) elect() {
	debugf("%s elect", r.tag)
	if r.State == StateLeader {
		//TODO : 是否需要加节点数限制.
		//if r.peerCount() == 1{
		log.Warnf("one node and already leader")
		//}
		return
	}
	//
	r.becomeCandidate()
	//如果只有一个节点，那么就直接是leader。
	if r.peerCount() == 1 {
		r.becomeLeader()
		return
	}
	//(5.2)broadcast the vote req
	var req = ReqVote{
		Term:         r.Term,
		CandidateId:  r.id,
		LastLogIndex: r.RaftLog.LastIndex(),
	}
	if req.LastLogIndex > 0 {
		var err error
		req.LastLogTerm, err = r.RaftLog.Term(req.LastLogIndex)
		if err != nil {
			log.Warnf("term(%d) err:%s", req.LastLogIndex, err.Error())
			return
		}
	}
	r.broadcast(&req)
}

func (r *Raft) doVote(to, curTerm, reqTerm uint64, voteGrant bool) {
	var rsp = &RspVote{
		Term:        curTerm,
		VoteGranted: voteGrant,
		//ReqTerm:     reqTerm,
	}
	if voteGrant {
		log.Debugf("%s vote %d-> %d for term(%d->%d)", r.tag, r.Vote, to, curTerm, r.Term)
		r.Vote = to
	} else {
		//log.TestLog("%s reject %d(%d<>%d)", r.tag, to, curTerm, reqTerm)
	}
	r.send(to, rsp)
}

//logs/1280:2020/09/02 18:09:13.609718 raft.go:338: [<test>error] raft-1280 become to leader(18)last(20);prs=Prs(5){1280:{20,21},1281:{20,21},1282:{20,21},1283:{20,21},1279:{20,21},};
//		votes=map[1280:true 1282:true 1283:true];raftLog{"applied":19,"commited":19,"stabled":20,"entries":[{"EntryNormal,6,6,dlen(0)},{"EntryNormal,6,7,dlen(60)}...<14>],"pendingSnapshot":nil};
//logs/1281:2020/09/02 18:09:13.944361 raft.go:338: [<test>error] raft-1281 become to leader(18)last(20);prs=Prs(5){1279:{20,21},1280:{20,21},1281:{20,21},1282:{20,21},1283:{20,21},};
//		votes=map[1279:true 1281:true 1283:true];raftLog{"applied":19,"commited":19,"stabled":20,"entries":[{"EntryNormal,6,6,dlen(0)},{"EntryNormal,6,7,dlen(60)}...<14>],"pendingSnapshot":nil};

func (r *Raft) onVote(m pb.Message) {
	curTerm := r.Term
	var rsp RspVote
	rsp.fromPbMsg(m)

	debugf("%s onVote '%d->%d'(%v):%v", r.tag, m.GetFrom(), m.GetTo(), m.GetMsgType(), rsp)
	//如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者（5.1 节）
	if rsp.Term > curTerm {
		r.becomeFollower(rsp.Term, 0)
		return
	}
	if r.State != StateCandidate {
		//如果已经不是Candidate了，不在乎选票了.
		debugf("'%d' was not candidate now.", r.id)
		return
	}
	//log.Infof("%s onVote '%d->%d'(%v):%v", r.tag, m.GetFrom(), m.GetTo(), m.GetMsgType(), rsp)
	//NOTICE-raft:如果不是本次Term，那么直接就drop，防止之前的vote消息有干扰.
	// 		这个是因为系统处理太慢导致的.
	//if rsp.ReqTerm != r.Term && rsp.ReqTerm != 0 {
	//	log.Warnf("%s Term(%d) resp.reqTerm(%d) err:message is too slow", r.tag, r.Term, rsp.ReqTerm)
	//	return
	//}
	if false == rsp.VoteGranted {
		//可能已经投票给其它人了
		r.votes[m.GetFrom()] = false
		rejectCnt := len(r.votes) - r.voteCount()
		if IsMajor(rejectCnt, r.peerCount()) {
			//如果已经有大多数人拒绝了，那么直接就失败了.
			r.becomeFollower(curTerm, 0)
		}
		return
	}
	r.votes[m.GetFrom()] = true
	//check vote major;
	if false == IsMajor(r.voteCount(), r.peerCount()) {
		//continue;
		return
	}
	//convert to leader;
	r.becomeLeader()
	//TODO : check - 论文说，每次选举为leader，都会立马发送一条空消息（心跳消息）；但是，这里实现，似乎说data为空都append消息。
	//r.Step(pb.Message{From: 0, To: 0, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	//r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})
}

func (r *Raft) handleVote(m pb.Message) {
	curTerm := r.Term
	//
	var req ReqVote
	req.fromPbMsg(m)

	log.Debugf("%s handleVote '%d->%d'(%v):%+v", r.tag, m.GetFrom(), m.GetTo(), m.GetMsgType(), req)

	//1.如果term < currentTerm返回 false （5.2 节）
	if req.Term < curTerm {
		log.Warnf("req.Term(%d) < curTerm(%d)", req.Term, curTerm)
		r.doVote(m.GetFrom(), curTerm, req.Term, false)
		return
	}
	// 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者（5.1 节）
	if req.Term > curTerm {
		r.becomeFollower(req.Term, 0)
		//这里仅仅是自己变为follower，因为有比自己大的term了;但是，还是需要比较log，如果日志不满足，那么会拒绝.
		if false == r.RaftLog.reqHasNewLog(&req) {
			debugf("log was not new")
			r.doVote(m.GetFrom(), curTerm, req.Term, false)
			return
		}
		r.doVote(m.GetFrom(), curTerm, req.Term, true)
		return
	}
	//下面处理term相等都情况.
	if false == r.RaftLog.reqHasNewLog(&req) {
		debugf("log was not new")
		r.doVote(m.GetFrom(), curTerm, req.Term, false)
		return
	}
	//3.如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
	//req.Term == curTerm
	if r.Vote != 0 && r.Vote != req.CandidateId {
		//已经投票给其它人了。
		debugf("vote others %d", r.Vote)
		r.doVote(req.CandidateId, curTerm, req.Term, false)
		return
	}
	r.doVote(req.CandidateId, curTerm, req.Term, true)
}

func (rl *RaftLog) reqHasNewLog(req *ReqVote) (isNew bool) {
	//	Raft 通过比较两份日志中最后一条日志条目的索引值和任期号定义谁的日志比较新（本质上就是要求日志是一致对）。
	//3.1 get last term/index;
	lastIndex := rl.LastIndex()
	if lastIndex == 0 {
		//如果系统刚刚启动，日志为空，那么请求日志必然是新等.
		return true
	}
	//check new logs;
	lastTerm, err := rl.Term(lastIndex)
	if err != nil {
		log.Errorf("last log term(%d) err:%s", lastIndex, err.Error())
		return false
	}
	//3.2 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新。
	if lastTerm < req.LastLogTerm {
		isNew = true
	}
	//3.3 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
	if lastTerm == req.LastLogTerm {
		if lastIndex <= req.LastLogIndex {
			isNew = true
		}
	}
	return isNew
}

func (r *Raft) voteCount() int {
	voteCnt := 0 //self vote self;
	for _, vote := range r.votes {
		if vote {
			voteCnt++
		}
	}
	if r.PendingConfIndex <= 0 {
		return voteCnt
	}
	//
	pos, err := r.RaftLog.pos(r.PendingConfIndex)
	if err != nil {
		log.Warnf("%s r.PendingConfIndex(%d) err:%s", r.tag, r.PendingConfIndex, err.Error())
		return voteCnt
	}
	conf := fromEntry(&r.RaftLog.entries[pos])
	if conf != nil {
		switch conf.ChangeType {
		case pb.ConfChangeType_AddNode:
			voteCnt++
		case pb.ConfChangeType_RemoveNode:
			voteCnt--
		default:
			log.Errorf("unknown ConfChange type:%v", conf.GetChangeType())
		}
	}
	return voteCnt
}
