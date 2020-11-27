package raft

import (
	"encoding/json"
	"fmt"
	"github.com/gogo/protobuf/sortkeys"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) onHeartbeatsFailed(to uint64, resp *RspHeartbeat, doAgain func(to uint64)) {
	//check term.
	if r.Term < resp.Term {
		r.becomeFollower(resp.Term, 0)
		return
	}
	//reset mach;
	pr := r.Prs[to]
	if pr.Match < 0 {
		log.Errorf("%s peer(%d)(term=%d)mach is zero.logic error.%+v;%s", r.tag, to, r.Term, resp, r.prs2string())
		return
	}
	//NOTICE-raft:如果不是本轮次到，直接忽略.
	//if pr.Match != resp.reqPrevLogIndex {
	//	log.Warnf("%s pr(%d):not this round message=%+v", r.tag, pr.Match, resp)
	//	return
	//}
	pr.Match--
	pr.Next = pr.Match + 1
	doAgain(to)
}

func (r *Raft) processHeartBeatRequest(req *ReqHeartbeat, resp *RspHeartbeat, from uint64) {
	curTerm := r.Term
	resp.Term = curTerm
	resp.Success = false
	//resp.reqPrevLogIndex = req.PrevLogIndex
	//resp.reqPrevLogTerm = req.PrevLogTerm
	//0. check args; 可能leader还没commit（没有大多数人收到日志，所以只能不commit，所以commit说可能小于prevLogIndex都。
	//if req.LeaderCommitId < req.PrevLogIndex {
	//	log.Errorf("req arg error commit(%d)<prevIndex(%d)", req.LeaderCommitId, req.PrevLogIndex)
	//	return
	//}
	//1.如果term < currentTerm返回 false （5.2 节）
	if req.Term < curTerm {
		log.Warnf("%s req.Term(%d)idx(%d)from(%d) < curTerm(%d)", r.tag, req.Term, req.PrevLogIndex, from, curTerm)
		return
	}
	// 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，那么就令 currentTerm 等于 T，并切换状态为跟随者（5.1 节）
	if req.Term > curTerm {
		//TODO ： 不管是否是follower，都需要更新term，所以索性都走这里。
		r.becomeFollower(req.Term, req.LeaderId)
	}
	//req.Term == curTerm
	if r.State == StateCandidate {
		//已经有leader，你就不要等选票支持了，没机会了。
		r.becomeFollower(req.Term, req.LeaderId)
	}
	if r.State == StateLeader {
		//同一term不可能有两个leader.
		//可能存在脑裂都情况，或者是transfer都情况,所以，这里将 Fatalf 改为 Warnf.drop this msg;
		log.Fatalf("%s logic error:state is leader.from=%d", r.tag, from)
		return
	}
	r.Lead = req.LeaderId
	//2.如果日志在 prevLogIndex 位置处的日志条目的任期号和 prevLogTerm 不匹配，则返回 false （5.3 节）
	//2.0 如果是带过来prev是空（0），那么表示ok。
	if req.PrevLogIndex == 0 {
		resp.Success = true
		return
	}
	//2.1 如果有日志，那么要比较日志.
	localTerm, err := r.RaftLog.Term(req.PrevLogIndex)
	if err != nil {
		//log.Warnf("term(idx:%d) err:%s", req.PrevLogIndex, err.Error())
		return //false
	}
	if localTerm != req.PrevLogTerm {
		log.Warnf("%s index(%d)localTerm(%d) != req.PrevLogTerm(%d)", r.tag, req.PrevLogIndex, localTerm, req.PrevLogTerm)
		return
	}
	resp.Success = true
}

func (r *Raft) processEntries(entries []*pb.Entry) bool {
	elen := len(entries)
	if elen == 0 { //no logs;
		return true
	}
	rlog := r.RaftLog
	startIdx := entries[0].Index
	//直接append
	if startIdx > rlog.LastIndex() {
		for _, e := range entries {
			rlog.entries = append(rlog.entries, *e)
		}
		return true
	}
	//有重叠日志，要检查重叠区域是否有冲突;(所有日志带index是连续带.)
	start, err := rlog.pos(entries[0].Index)
	if err != nil {
		log.Errorf("index(%d) error:%s", entries[0].Index, err.Error())
		return false
	}
	//find the conflict pos;
	logLen := len(rlog.entries)
	inpos := 0
	for ; start < uint64(logLen) && inpos < elen; start++ {
		//check conflict;
		if rlog.entries[start].Term != entries[inpos].Term {
			//delete the conflict position logs;
			rlog.entries = rlog.entries[:start]
			if len(rlog.entries) > 0 {
				//日志有冲突，那么就需要重新saveLog
				last := len(rlog.entries) - 1
				ent := &rlog.entries[last]
				if rlog.stabled > ent.Index {
					rlog.stabled = ent.Index
				}
			} else {
				rlog.stabled = entries[inpos].Index - 1
			}
			break //(start,inpos) is conflict position;
		} else {
			inpos++
		}
	}

	//debugf("do append at inpos(%d)",inpos)
	for ; inpos < elen; inpos++ {
		rlog.entries = append(rlog.entries, *entries[inpos])
	}
	return true
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	var req ReqHeartbeat
	req.fromPbMsg(m)
	if req.LeaderCommitId == 0 {
		return
	}
	var resp RspHeartbeat
	debugf("handleHeartbeat '%d->%d'(%v):%v", m.GetFrom(), m.GetTo(), m.GetMsgType(), req)
	r.processHeartBeatRequest(&req, &resp, m.GetFrom())
	if false == resp.Success {
		r.send(m.GetFrom(), &resp)
		return
	}
	//3.如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 （5.3 节）
	//4.附加日志中尚未存在的任何新条目
	//5.如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
	cidx := min(req.LeaderCommitId, req.PrevLogIndex)
	if cidx > r.RaftLog.committed {
		r.RaftLog.committed = cidx
	}
	r.send(m.GetFrom(), &resp)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) onHeartbeat(m pb.Message) {
	if false == r.isLeader() {
		//可能落后的消息,直接drop.
		return
	}
	// Your Code Here (2A).
	var resp RspHeartbeat
	resp.fromPbMsg(m)
	debugf("onHeartbeat '%d->%d':%+v", m.GetFrom(), m.GetTo(), resp)
	//失败处理.
	if false == resp.Success {
		r.onHeartbeatsFailed(m.GetFrom(), &resp, func(to uint64) {
			r.sendHeartbeat(to)
		})
		return
	}
	//成功处理.
}

func (r *Raft) preProcessAppendEntries(req *ReqAppend) {
	elen := len(req.Entries)
	if elen == 0 {
		return
	}
	//NOTICE-3B:虽然leader地方已经过滤来一次有两个confChange.
	//  但是，leader是不会知道，follower这边可能有一个confChange正在进行时
	//  上次到到这里仅仅是commit状态，要这一轮才会apply，所以需要做下校验
	//  这里选择直接truncate req.Entries，考虑到下次还可以继续sendAppend.
	//1- PendingConfIndex 没有值.
	if r.PendingConfIndex == 0 {
		for idx, ent := range req.Entries {
			if ent.EntryType == pb.EntryType_EntryConfChange {
				if r.PendingConfIndex > 0 {
					//已经存在，那么就截断.
					req.Entries = req.Entries[:idx]
					return
				} else {
					r.PendingConfIndex = ent.Index
				}
			} //end confChange
		}
		return
	}
	//2- PendingConfIndex 已经有值，那么需要跟entries对比.
	first := req.Entries[0]
	if first.Index > r.PendingConfIndex {
		//只需要如果有ConfChange，直接截断.
		for idx, ent := range req.Entries {
			if ent.EntryType == pb.EntryType_EntryConfChange {
				req.Entries = req.Entries[:idx]
				return
			}
		}
		return
	}
	//3- PendingConfIndex > first,说明新日志和老日志存在冲突，由于pending还没有apply，直接置0就可以来。
	r.PendingConfIndex = 0
	for idx, ent := range req.Entries {
		if ent.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex > 0 {
				//已经存在，那么就截断.
				req.Entries = req.Entries[:idx]
				return
			} else {
				r.PendingConfIndex = ent.Index
			}
		} //end confChange
	}
	return
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	var req ReqAppend
	req.fromPbMsg(m)
	r.preProcessAppendEntries(&req)
	//for log
	req.Entries = nil
	debugf("handleAppendEntries '%d->%d'(%v):%+v(ents=%d)", m.GetFrom(), m.GetTo(), m.GetMsgType(), req, len(m.GetEntries()))
	req.Entries = m.GetEntries()

	var resp RspAppend
	r.processHeartBeatRequest(&req.ReqHeartbeat, &resp.RspHeartbeat, m.GetFrom())
	if false == resp.Success {
		r.send(m.GetFrom(), &resp)
		return
	}

	//3.如果已经存在的日志条目和新的产生冲突（索引值相同但是任期号不同），删除这一条和之后所有的 （5.3 节）
	//4.附加日志中尚未存在的任何新条目
	if false == r.processEntries(m.GetEntries()) {
		resp.Success = false
		r.send(m.GetFrom(), &resp)
		return
	}
	//
	resp.LastLogIndex = req.PrevLogIndex + uint64(len(req.Entries))
	if resp.LastLogIndex > r.RaftLog.LastIndex() {
		log.Warnf("resp.LastLogIndex<%d> != r.RaftLog.LastIndex()<%d>", resp.LastLogIndex, r.RaftLog.LastIndex())
	}
	//5.如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
	//	raftLog.entries 比 req.entries 日志多，并且没有冲突的话,那么resp.LastLogIndex < raftLog.entries的.
	//	但是，commit以leader给的日志为准，所以是resp的最大日志（prevLogIndex + len(req.entries)计算）.
	cidx := min(resp.LastLogIndex, req.LeaderCommitId)
	//debugf("set commit=%d", cidx)
	if cidx > r.RaftLog.committed {
		r.RaftLog.committed = cidx
	}

	r.send(m.GetFrom(), &resp)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) onAppendEntries(m pb.Message) {
	if false == r.isLeader() {
		//可能落后的消息,直接drop.
		return
	}
	// Your Code Here (2A).
	var resp RspAppend
	resp.fromPbMsg(m)
	debugf("onAppendEntries '%d->%d'(%v):%+v", m.GetFrom(), m.GetTo(), m.GetMsgType(), resp)
	//失败处理。
	if false == resp.Success {
		r.onHeartbeatsFailed(m.GetFrom(), &resp.RspHeartbeat, func(to uint64) {
			r.sendAppend(to)
		})
		return
	}
	//成功处理.
	//1-update progress;
	pr := r.Prs[m.GetFrom()]
	pr.Match = resp.LastLogIndex
	pr.Next = pr.Match + 1
	if pr.Match < r.RaftLog.LastIndex() {
		//还有没发的日志，再次发送.
		r.sendAppend(m.GetFrom())
	}
	//leadership transfer state going.
	if pr.Match == r.RaftLog.LastIndex() && r.leadTransferee == m.GetFrom() {
		r.sendTimeoutNow(m.GetFrom())
	}
	//2-update raftLog commit;
	r.updatePrCommits()
	return
}

func (r *Raft) updatePrCommits() {
	if r.State != StateLeader {
		return
	}
	//	如果存在一个满足N > commitIndex的 N，并且大多数的matchIndex[i] ≥ N成立，并且log[N].term == currentTerm成立，那么令 commitIndex 等于这个 N （5.3 和 5.4 节）
	var counters = map[uint64]int{}
	//counters[r.RaftLog.LastIndex()] = 1
	for _, pr := range r.Prs {
		v := counters[pr.Match]
		counters[pr.Match] = v + 1
	}
	sortLogIndex := make([]uint64, 0, len(counters))
	for logidx, _ := range counters {
		if logidx > r.RaftLog.committed {
			sortLogIndex = append(sortLogIndex, logidx)
		}
	}
	sortkeys.Uint64s(sortLogIndex)
	prvCnt := 0
	last := len(sortLogIndex) - 1
	total := r.peerCount()
	for ; last >= 0; last-- {
		logidx := sortLogIndex[last]
		if logidx < r.RaftLog.committed {
			//可能会存在落后很多多节点，所以，需要判断下.
			break
		}
		cnt := counters[logidx] + prvCnt
		if IsMajor(cnt, total) {
			//并且log[N].term == currentTerm成立(就是说，不是自己的，不commit)
			term, err := r.RaftLog.Term(logidx)
			if err != nil {
				log.Warnf("%s commit err:term(%d) err:%s", r.tag, logidx, err.Error())
				d, _ := json.Marshal(r.Prs)
				log.Fatalf("%s leader(%d) total=%d;cnt=%d;prevCnt=%d;prs=%+v;logIdxs=%+v;counters=%+v", r.tag, r.Lead, total, cnt, prvCnt, string(d), sortLogIndex, counters)
			} else {
				if term == r.Term {
					if r.RaftLog.committed < logidx {
						r.RaftLog.committed = logidx
						//update the commit id to follower;
						debugf("%s update commit %d", r.tag, r.RaftLog.committed)
						r.broadcastAppend()
					}
				}
			}
			return
		} else {
			prvCnt = cnt
		}
	}
}

var ErrNeedLeader = fmt.Errorf("need leader can do")

func (r *Raft) makeHeartbeat(pr *Progress, hb *ReqHeartbeat) error {
	if r.State != StateLeader {
		return ErrNeedLeader
	}
	hb.Term = r.Term
	hb.LeaderId = r.id
	hb.LeaderCommitId = r.RaftLog.committed
	hb.PrevLogIndex = pr.Match
	t, err := r.RaftLog.Term(pr.Match)
	if err != nil {
		return err
	}
	hb.PrevLogTerm = t
	return nil
}
