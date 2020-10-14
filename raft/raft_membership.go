package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

//The New configuration takes effect on each server as soon as it is added to the server's log
//只需要appendEntry，就已经生效.(通过pendingConfIndex)

//raft 协议作者 Diego 在其博士论文中已经详细介绍了Single Cluser MemberShip Change 机制，包括Security、Avaliable方面的详细说明， 并且作者也说明了在实际工程实现过程中更加推荐Single方式，
//  首先因为简单，
//  再则所有的集群变更方式都可以通过Single 一次一个节点的方式达到任何想要的Cluster 状态。
//func (r *Raft) updatePendingConfByAppendEntries(entries []*pb.Entry) {
//	if r.PendingConfIndex > 0 {
//		return
//	}
//	prevPos := 0
//	for i := 0; i < len(entries); i++ {
//		ent := entries[i]
//		if ent.EntryType == pb.EntryType_EntryConfChange {
//			if r.PendingConfIndex > 0 {
//				prev := fromEntry(entries[prevPos])
//				now := fromEntry(ent)
//				log.Warnf(`multi confChange in raft system.prev(%v-->%d) execute and drop now(%v-->%d)`, prev.ChangeType, prev.NodeId, now.ChangeType, now.NodeId)
//			} else {
//				r.pendingConf = fromEntry(ent)
//				if r.pendingConf == nil {
//					panic(nil)
//				}
//				r.PendingConfIndex = ent.Index
//				prevPos = i
//			}
//		}
//	}
//	//if r.PendingConfIndex == 0{
//	//	if r.pendingConf.ChangeType == pb.ConfChangeType_AddNode{
//	//		if
//	//	}
//	//}
//}

//本节点是否还在raft系统中?
func (r *Raft) isInRaft(id uint64) bool {
	_, ok := r.Prs[id]
	return ok
}

func fromEntry(ent *pb.Entry) *pb.ConfChange {
	if ent.GetEntryType() != pb.EntryType_EntryConfChange {
		log.Warnf(`fromEntry was not ConfChange`)
		return nil
	}
	var cc pb.ConfChange
	err := cc.Unmarshal(ent.Data)
	if err != nil {
		log.Errorf(`fromEntry error:%s.`, err.Error())
		return nil
	}
	return &cc
}

func (r *Raft) sendTimeoutNow(transfee uint64) {
	pr := r.Prs[transfee]
	var hb ReqHeartbeat
	err := r.makeHeartbeat(pr, &hb)
	if err != nil {
		//
		log.Warnf(`%s handleTransferLeader(%d) err:%s.sendAppend`, r.tag, transfee, err.Error())
		r.sendAppend(transfee)
		return
	}
	//send to `to`;
	toMsg := hb.toPbMsg()
	toMsg.MsgType = pb.MessageType_MsgTimeoutNow
	r.sendPb(transfee, toMsg)
	r.leadTransferee = None
}

func (r *Raft) ResetPrs() {
	r.Prs = map[uint64]*Progress{
		r.id: &Progress{0, 0},
	}
}
