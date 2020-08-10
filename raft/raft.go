// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"time"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	electionTick    int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

func (r *Raft) String() string {
	return fmt.Sprintf("{id:%d term:%d %v prs:%d log:%s}", r.id, r.Term, r.State, len(r.Prs), r.RaftLog.String())
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := new(Raft)
	//init raft-log;
	r.RaftLog = newLog(c.Storage)
	if c.Applied > 0 {
		r.RaftLog.applied = c.Applied
		if r.RaftLog.committed < c.Applied {
			r.RaftLog.committed = c.Applied
		}
	}
	//others;
	r.id = c.ID
	r.heartbeatTimeout = c.HeartbeatTick
	//r.electionTimeout = c.ElectionTick
	r.electionTick = c.ElectionTick
	r.State = StateFollower
	hard, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	r.Vote = hard.Vote
	r.Term = hard.Term
	if hard.Commit > 0 {
		r.RaftLog.committed = hard.Commit
	}
	//init random;
	rand.Seed(time.Now().UnixNano())
	r.randomElection()
	//init prs;
	//bug fix check;
	if len(c.peers) <= 0 {
		c.peers = confState.Nodes
	}
	r.Prs = make(map[uint64]*Progress, len(c.peers))
	for _, peer := range c.peers {
		r.Prs[peer] = &Progress{}
	}
	debugf("%s", r.String())
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	var req ReqAppend
	pr := r.Prs[to]
	err := r.makeHeartbeat(pr, &req.ReqHeartbeat)
	if err != nil {
		if err == ErrCompacted {
			r.sendSnapshot(to)
		}
		log.Warnf("makeHeartbeat(%d) error:%s", pr.Match, err.Error())
		return false
	}
	//get entries;
	ents, err := r.RaftLog.startAt(pr.Next)
	if err != nil {
		//log.Warnf("startAt(%d) error:%s", pr.Next, err.Error())
		//TODO : check --- 如果没有entry，是不是直接发送心跳好了.
	} else {
		req.copyEntries(ents)
	}
	r.send(to, &req)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	var req ReqHeartbeat
	pr := r.Prs[to]
	err := r.makeHeartbeat(pr, &req)
	if err == nil {
		r.send(to, &req)
	} else {
		log.Warnf("makeHeartbeat(%d,%v) error:%s", to, pr, err.Error())
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			//TODO : do heartbeat;
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed >= r.electionTimeout {
			//TODO : do elect;
			r.elect()
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.Vote = lead
	//election reset;
	r.electionElapsed = 0
	r.randomElection()
	log.Infof("%d become to follower(%d) vote(%d) lastIndex(%d)", r.id, r.Term, lead, r.RaftLog.LastIndex())
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() { //可能是再次选举。
	// Your Code Here (2A).
	//(5.2)自增当前的任期号（currentTerm）
	r.Term++
	r.State = StateCandidate
	//(5.2)给自己投票
	r.Vote = r.id
	r.Lead = 0
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	//(5.2)重置选举超时计时器
	r.electionElapsed = 0
	r.randomElection()
	log.Infof("%d goto election(%d)", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.votes = nil
	r.Lead = r.id
	r.heartbeatTimeout = 0
	//选举后重新初始化(progress)
	for _, pr := range r.Prs {
		pr.Match = r.RaftLog.LastIndex()
		pr.Next = r.RaftLog.LastIndex() + 1 //初始化为领导人最后索引值加一
	}
	log.Infof("%d become to leader: term(%d)index(%d)", r.id, r.Term, r.RaftLog.LastIndex())
	//log.Warnf("%d become to leader(%d)last(%d)raftLog%s", r.id, r.Term, r.RaftLog.LastIndex(), r.RaftLog.String())
	//TODO : check - 论文说，每次选举为leader，都会立马发送一条空消息（心跳消息）；但是，这里实现，似乎说data为空都append消息。
	r.Step(pb.Message{From: 0, To: 0, MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{{}}})
	//r.Step(pb.Message{From: 1, To: 1, MsgType: pb.MessageType_MsgBeat})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if IsLocalMsg(m.MsgType) {
		return r.handleLocal(m)
	}

	return r.handleRemote(m)
}

func snap2str(m *pb.Message) string {
	sp := m.GetSnapshot()
	if sp == nil {
		return fmt.Sprintf(`'%d -> %d' %v snapshot nil`, m.GetFrom(), m.GetTo(), m.GetMsgType())
	}
	md := sp.GetMetadata()
	if md == nil {
		return fmt.Sprintf(`'%d -> %d' %v snapshot{ data {%d} metadata nil}`, m.GetFrom(), m.GetTo(), m.GetMsgType(), len(sp.GetData()))
	}
	return fmt.Sprintf(`'%d -> %d' %v snapshot{ data {%d} metadata {%+v}}`, m.GetFrom(), m.GetTo(), m.GetMsgType(), len(sp.GetData()), md)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	//do check;
	if m.GetSnapshot() == nil {
		log.Warnf("handleSnapshot %s", snap2str(&m))
	}
	if m.GetSnapshot().GetMetadata() == nil {
		log.Warnf("handleSnapshot %s", snap2str(&m))
	}
	debugf("handleSnapshot %s", snap2str(&m))
	//term	领导人的任期号
	//leaderId	领导人的 Id，以便于跟随者重定向请求
	//lastIncludedIndex	快照中包含的最后日志条目的索引值
	//lastIncludedTerm	快照中包含的最后日志条目的任期号
	//offset	分块在快照中的字节偏移量
	//data[]	从偏移量开始的快照分块的原始字节
	//done	如果这是最后一个分块则为 true
	term := m.GetTerm()
	leaderId := m.GetFrom()
	md := m.GetSnapshot().Metadata
	lastIndex := md.GetIndex()
	//lastTerm := md.GetTerm()
	//data := m.GetSnapshot().GetData()
	//1-如果term < currentTerm就立即回复
	var rsp pb.Message
	rsp.To = m.GetFrom()
	rsp.Term = r.Term
	if term < r.Term {
		r.sendPb(rsp.To, rsp)
		return
	}
	if lastIndex < r.RaftLog.LastIndex() {
		log.Error("logic error:snap.lastIndex(%d)<local.lastIndex(%d)", lastIndex, r.RaftLog.LastIndex())
		r.sendPb(rsp.To, rsp)
		return
	}
	//set pendingSnapshot
	if r.RaftLog.pendingSnapshot == nil {
		r.RaftLog.pendingSnapshot = m.GetSnapshot()
		r.RaftLog.maybeCompact()
	} else {
		//check old/new
		old := r.RaftLog.pendingSnapshot.GetMetadata()
		newmd := m.GetSnapshot().GetMetadata()
		if old.GetIndex() < newmd.GetIndex() {
			r.RaftLog.pendingSnapshot = m.GetSnapshot()
			r.RaftLog.maybeCompact()
		} else {
			return
		}
	}
	r.becomeFollower(term, leaderId)
	nodes := m.GetSnapshot().GetMetadata().ConfState
	r.Prs = map[uint64]*Progress{}
	for _, nd := range nodes.GetNodes() {
		r.Prs[nd] = &Progress{}
	}

	r.sendPb(rsp.To, rsp)
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

//other help functions;
func (r *Raft) sendPb(to uint64, m pb.Message) {
	if m.GetFrom() == 0 {
		m.From = r.id
	}
	m.To = to
	r.msgs = append(r.msgs, m)
}

func (r *Raft) send(to uint64, m message) {
	pbm := m.toPbMsg()
	r.sendPb(to, pbm)
	if m2, ok := m.(*ReqAppend); ok {
		elen := len(m2.Entries)
		m2.Entries = nil
		debugf("send '%d->%d'(%v):%+v(ents=%d)", r.id, to, pbm.GetMsgType(), m2, elen)
	} else {
		debugf("send '%d->%d'(%v):%+v", r.id, to, pbm.GetMsgType(), m)
	}
}

func (r *Raft) sendSnapshot(to uint64) {
	sp, err := r.RaftLog.storage.Snapshot()
	if err != nil {
		log.Error("send snapshot err:%s", err.Error())
		return
	}
	msg := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &sp,
	}
	r.sendPb(to, msg)
}

func (r *Raft) broadcast(m message) {
	for to, _ := range r.Prs {
		if to == r.id {
			continue
		}
		r.send(to, m)
	}
}

func (r *Raft) randomElection() {
	r.electionTimeout = r.electionTick + rand.Intn(r.electionTick)
}

func (r *Raft) peerCount() int {
	return len(r.Prs) //count self node;
}

func (r *Raft) handleRemote(m pb.Message) error {
	//handle net message;
	if r.State == StateFollower {
		//收到消息，就重置——如果一个跟随者在一段时间里没有接收到任何消息，也就是选举超时
		r.electionElapsed = 0
	}
	//process;
	switch m.GetMsgType() {
	case pb.MessageType_MsgRequestVote:
		r.handleVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.onVote(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.onAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.onHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	default:
		log.Warnf("Step(%v) was not support", m.GetMsgType())
	}
	return nil
}

func (r *Raft) handleLocal(m pb.Message) error {

	switch m.GetMsgType() {
	case pb.MessageType_MsgHup:
		r.handleHup(m)
	case pb.MessageType_MsgBeat:
		r.handleBeat(m)
	case pb.MessageType_MsgPropose:
		r.handlePropose(m)
	}
	return nil
}

func (r *Raft) handlePropose(m pb.Message) {
	if r.State != StateLeader {
		log.Warnf("state(%v) was not leader!can not Propose('%d->%d'(ents=%d))", r.State, r.id, m.GetTo(), len(m.GetEntries()))
		return
	}
	debugf("'%d->%d'%v(ents=%d)", r.id, m.GetTo(), m.GetMsgType(), len(m.GetEntries()))
	//append logs;
	rlog := r.RaftLog
	lastIndex := rlog.LastIndex()
	for _, e := range m.GetEntries() {
		//if e.Data != nil {
		e.Term = r.Term
		e.Index = lastIndex + 1
		rlog.entries = append(rlog.entries, *e)
		//}
	}
	if r.peerCount() == 1 {
		rlog.committed = rlog.LastIndex()
		debugf("only 1 node,set commit=%d", rlog.committed)
		return
	}
	r.broadcastAppend()
}

func (r *Raft) broadcastAppend() {
	rlog := r.RaftLog
	for to, pr := range r.Prs {
		if to == r.id {
			pr.Match = rlog.LastIndex()
			pr.Next = pr.Match + 1
		} else {
			r.sendAppend(to)
		}
	}
}

func (r *Raft) handleBeat(m pb.Message) {
	if r.State != StateLeader {
		log.Warnf("I'm(%d) not leader", r.id)
		return
	}
	debugf("%d %v", r.id, m.GetMsgType())
	if r.peerCount() == 1 {
		return
	}
	for to, _ := range r.Prs {
		if to == r.id {
			continue
		}
		r.sendHeartbeat(to)
	}
}

func (r *Raft) handleHup(m pb.Message) {
	debugf("%d %v", r.id, m.GetMsgType())
	r.elect()
}
