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
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft *Raft
	// Your Data Here (2A).
	prevReady Ready
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	// Your Code Here (2A).
	rn := &RawNode{Raft: newRaft(config)}
	rn.prevReady = makeReadyState(rn.Raft)
	return rn, nil
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receivinover network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) Ready() (rd Ready) {
	// Your Code Here (2A).
	r := rn.Raft
	debugf("Ready(%s)", r.RaftLog.String())
	//soft state;
	newrd := makeReadyState(r)
	rn.readyState(&rd, &rn.prevReady, &newrd)
	//
	rd.Entries = r.RaftLog.unstableEntries()
	//TODO (snapshot) : to do later;
	if r.RaftLog.pendingSnapshot != nil {
		rd.Snapshot = *r.RaftLog.pendingSnapshot
	}
	rd.CommittedEntries = r.RaftLog.nextEnts()
	//fetch messages;
	rd.Messages = r.msgs
	r.msgs = []pb.Message{}
	return rd
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	if rn.Raft.RaftLog.pendingSnapshot != nil {
		return true
	}
	newrd := makeReadyState(rn.Raft)
	//debugf("hashReady:old=%s;new=%s;", state2str(&rn.prevReady), state2str(&newrd))
	if false == sameSoftState(&newrd, &rn.prevReady) {
		//log.Debugf("softState %d", len(rn.Raft.msgs))
		return true
	}
	if false == sameHardState(&newrd, &rn.prevReady) {
		//log.Debugf("hardState %d", len(rn.Raft.msgs))
		return true
	}
	if len(rn.Raft.msgs) > 0 {
		//log.Debugf("messages")
		return true
	}
	rlog := rn.Raft.RaftLog
	if rlog.LastIndex() <= rlog.stabled {
		return rlog.committed > rlog.applied
	}
	return true
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	debugf("Advance(hard=%+v;soft=%+v)", rd.HardState, rd.SoftState)
	// Your Code Here (2A).
	rlog := rn.Raft.RaftLog
	rlog.applied += uint64(len(rd.CommittedEntries))
	rlog.stabled += uint64(len(rd.Entries))
	if rlog.stabled < rlog.applied {
		str := ""
		if rd.Snapshot.GetMetadata() != nil {
			md := rd.Snapshot.GetMetadata()
			str = fmt.Sprintf(`snapshot{%d,%d};`, md.GetIndex(), md.GetTerm())
		}
		log.Fatalf(`%s commit=%d;enties=%d;%srlog{%s}`, rn.Raft.tag, len(rd.CommittedEntries), len(rd.Entries), str, rlog.String())
	}
	//
	if false == emptySoftState(&rd) && false == sameSoftState(&rd, &rn.prevReady) {
		rn.prevReady.RaftState = rd.SoftState.RaftState
		rn.prevReady.Lead = rd.SoftState.Lead
	}
	if false == emptyHardState(&rd) && false == sameHardState(&rd, &rn.prevReady) {
		rn.prevReady.HardState = rd.HardState
	}
	//TODO (snapshot) : to do later;
	rlog.pendingSnapshot = nil //reset;
	rlog.maybeCompact(rn.RaftID())
}

// GetProgress return the the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}

func (rn *RawNode) RaftID() uint64 {
	return rn.Raft.id
}

//----------------------------------------------------
func makeReadyState(r *Raft) (rd Ready) {
	rd.SoftState = &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
	//hardstate;
	rd.HardState.Commit = r.RaftLog.committed
	rd.HardState.Term = r.Term
	rd.HardState.Vote = r.Vote
	return
}

func sameSoftState(r *Ready, b *Ready) bool {
	return r.Lead == b.Lead && r.RaftState == b.RaftState
}

func sameHardState(r *Ready, b *Ready) bool {
	return r.Vote == b.Vote && r.Term == b.Term && r.Commit == b.Commit
}

func emptyHardState(r *Ready) bool {
	hs := r.HardState
	return hs.Vote == 0 && hs.Commit == 0 && hs.Term == 0
}

func emptySoftState(r *Ready) bool {
	ss := r.SoftState
	if ss == nil {
		return true
	}
	return ss.Lead == 0 && ss.RaftState == 0
}

func state2str(rd *Ready) string {
	return fmt.Sprintf("softState=%+v;HardState=%+v.", rd.SoftState, rd.HardState)
}

func (rn *RawNode) readyState(set, old, newrd *Ready) {
	debugf("old(%s) new(%s)", old, newrd)
	r := rn.Raft
	//check if changed;
	if false == sameSoftState(old, newrd) {
		set.SoftState = &SoftState{
			Lead:      r.Lead,
			RaftState: r.State,
		}
	}
	//hardstate;
	if false == sameHardState(old, newrd) {
		set.HardState.Commit = r.RaftLog.committed
		set.HardState.Term = r.Term
		set.HardState.Vote = r.Vote
	}
}
