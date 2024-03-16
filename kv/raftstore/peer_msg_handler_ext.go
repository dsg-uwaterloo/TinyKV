package raftstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/gogo/protobuf/sortkeys"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	"github.com/pingcap-incubator/tinykv/raft"

	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

func (d *peerMsgHandler) SetRegion(region *metapb.Region) {
	//NOTICE-3B:这里要更新两个地方，具体看函数内部实现.
	d.ctx.storeMeta.setRegion(region, d.peer)
}

func propose2Str(msg *eraftpb.Message) string {
	return fmt.Sprintf(`{%v Term:%d Log:{Term:%d Index:%d} commit:%d Reject:%v entries:%d snapshot:%v}`,
		msg.GetMsgType(), msg.GetTerm(), msg.GetLogTerm(), msg.GetIndex(), msg.GetCommit(),
		msg.GetReject(), len(msg.GetEntries()), msg.GetSnapshot())
}

func RaftMsg2Str(msg *rspb.RaftMessage) string {
	rmsg := msg.GetMessage()
	return fmt.Sprintf(`{ regionID:%d '%d->%d' message:%v Epoch:%+v IsTombstone:%v "'%s'->'%s'"}`,
		msg.GetRegionId(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId(),
		propose2Str(rmsg), msg.GetRegionEpoch(), msg.GetIsTombstone(), string(msg.GetStartKey()), string(msg.GetEndKey()))
}

var msgIndex uint64

func nextMsgIndex() uint64 {
	return atomic.AddUint64(&msgIndex, 1)
}

//type raftmsg interface {
//	MarshalTo(dAtA []byte) (int, error)
//	Size() int
//	Unmarshal(dAtA []byte) error
//}

type RaftMsgWrapper struct {
	//raftmsg
	//MsgTerm uint64
	MsgIdx  uint64
	raftmsg *raft_cmdpb.RaftCmdRequest
}

func (rmw *RaftMsgWrapper) String() string {
	return fmt.Sprintf("req_%d_%d", rmw.MsgIdx, rmw.raftmsg.GetHeader().GetTerm())
}

const termOff = 8

func (rmw *RaftMsgWrapper) Marshal() ([]byte, error) {
	n := rmw.raftmsg.Size() + termOff
	buf := make([]byte, n)
	binary.BigEndian.PutUint64(buf[:termOff], rmw.MsgIdx)
	_, err := rmw.raftmsg.MarshalTo(buf[termOff:])
	return buf, err
}

func (rmw *RaftMsgWrapper) Unmarshal(buf []byte) error {
	var req raft_cmdpb.RaftCmdRequest
	err := req.Unmarshal(buf[termOff:])
	if err != nil {
		return err
	}
	rmw.raftmsg = &req
	rmw.MsgIdx = binary.BigEndian.Uint64(buf[:termOff])
	return nil
}

// NOTICE-3B:这里本质上来说，只需要一个peer进行序列化就可以来，因为add只需要这个peer都store信息。
//
//	但是，这里加过index说为来记录下，看看一个confChange要执行几次。
//	另外，confChange是有可能执行多次的——因为需要raft共识，导致region对比异常，则需要再次来添加、删除节点.
type ConfChange struct {
	*metapb.Peer
	Index uint64
}

func (cc *ConfChange) String() string {
	return fmt.Sprintf("CC{%d-%d,%d}", cc.Peer.GetId(), cc.Peer.GetStoreId(), cc.Index)
}

func (rmw *ConfChange) Marshal() ([]byte, error) {
	ccIndex++
	rmw.Index = ccIndex
	n := rmw.Peer.Size() + termOff
	buf := make([]byte, n)
	binary.BigEndian.PutUint64(buf[:termOff], rmw.Index)
	_, err := rmw.Peer.MarshalTo(buf[termOff:])
	return buf, err
}

func (rmw *ConfChange) Unmarshal(buf []byte) error {
	if buf == nil {
		return fmt.Errorf("buf is nil")
	}
	var req metapb.Peer
	err := req.Unmarshal(buf[termOff:])
	if err != nil {
		return err
	}
	rmw.Peer = &req
	rmw.Index = binary.BigEndian.Uint64(buf[:termOff])
	return nil
}

//------------------------------------------------------

func (d *peerMsgHandler) raftPropose(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	var rmw = RaftMsgWrapper{
		raftmsg: msg,
		MsgIdx:  nextMsgIndex(),
	}
	util.RSDebugf("raftPropose(%s)", rmw.String())
	data, err := rmw.Marshal()
	if err != nil {
		log.Errorf("raftPropose Marshal err:%s", err.Error())
		cb.Done(ErrResp(err))
		return
	}
	//record;
	d.peer.pushCallback(&rmw, cb)
	//raft process;
	err = d.peer.RaftGroup.Propose(data)
	if err != nil {
		log.Errorf("raftPropose Propose err:%s", err.Error())
		cb.Done(ErrResp(err))
		d.peer.popCallback(&rmw)
		return
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	raftGroup := d.peer.RaftGroup
	if !raftGroup.HasReady() {
		return
	}
	//1.stabled;
	//d.saveRaftLog(&rd)
	util.RSDebugf("%s HandleRaftReady", d.peer.Tag)
	rd := raftGroup.Ready()

	if len(rd.Entries) > 0 {
		// log.Infof("Number of Entries is: %d", len(rd.Entries))
		for idx := 0; idx < len(rd.Entries); idx++ {

			ent := &rd.Entries[idx]

			d.processEntry(ent)

		}
	}

	_, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		panic(err)
	}
	if rd.Snapshot.GetMetadata() != nil {
		d.ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
		d.SetRegion(d.Region())
	}
	//3.processSnapshot;
	//2.send msg to peers;
	d.sendRaftMsg(&rd)
	//4.apply;
	if len(rd.CommittedEntries) > 0 {
		util.RSDebugf("%s HandleRaftReady apply entries(%d)", d.Tag, len(rd.CommittedEntries))
		for idx := 0; idx < len(rd.CommittedEntries); idx++ {
			ent := &rd.CommittedEntries[idx]
			if ent.EntryType == eraftpb.EntryType_EntryConfChange {
				//TODO : raftGroup.ProposeConfChange()
				var cc eraftpb.ConfChange
				err := cc.Unmarshal(ent.GetData())
				if err != nil {
					log.Fatalf(`apply confChange err:%s`, err.Error())
				}
				d.updateConfChange(&cc)
				d.RaftGroup.ApplyConfChange(cc)
			} else {
				d.processEntry(ent)
			}
		}
	}
	//5.at last;
	raftGroup.Advance(rd)
}

func (d *peerMsgHandler) sendRaftMsg(rd *raft.Ready) {
	if len(rd.Messages) == 0 {
		return
	}
	util.RSDebugf("%s sendRaftMsg messages(%d)", d.peer.Tag, len(rd.Messages))
	region := d.Region()
	for idx := 0; idx < len(rd.Messages); idx++ {
		msg := &rd.Messages[idx]
		rmsg := &rspb.RaftMessage{
			RegionId:    region.GetId(),
			FromPeer:    d.peer.Meta,
			ToPeer:      d.peer.getPeerFromCache(msg.To),
			Message:     msg,
			RegionEpoch: region.GetRegionEpoch(),
			StartKey:    region.GetStartKey(),
			EndKey:      region.GetEndKey(),
		}
		err := d.ctx.trans.Send(rmsg)
		if !util.IsHeartbeatMsg(msg.GetMsgType()) {
			if err != nil {
				log.Warnf("%s '%d->%d' msg(%v)error:%v", d.Tag, msg.GetFrom(), msg.GetTo(), msg.GetMsgType(), err)
			} else {
				util.RSDebugf("%s '%d->%d' msg(%v)", d.Tag, msg.GetFrom(), msg.GetTo(), msg.GetMsgType())
			}
		}
	}
}

func (d *peerMsgHandler) saveRaftLog(rd *raft.Ready) {
	if len(rd.Entries) == 0 {
		return
	}
	ent0 := rd.Entries[0]
	util.RSDebugf("%s saveRaftLog entries(%d:%d;%d)", d.Tag, ent0.Term, ent0.Index, len(rd.Entries))
	var raftwb engine_util.WriteBatch
	err := d.peer.peerStorage.Append(rd.Entries, &raftwb)
	if err != nil {
		log.Fatalf("saveRaftLog store.Append err:%s", err.Error())
		return
	}
	raftwb.WriteToDB(d.ctx.engine.Raft)
}

type CbWrapper struct {
	*message.Callback
}

func (cb *CbWrapper) Done(resp *raft_cmdpb.RaftCmdResponse) {
	if cb.Callback != nil {
		cb.Callback.Done(resp)
	} else {
		//可能是其它节点(not leader)，不需要callback.
	}
}

func isReadRequest(msg *raft_cmdpb.RaftCmdRequest) bool {
	if len(msg.GetRequests()) == 0 {
		return false
	}
	cmdtype := msg.GetRequests()[0].GetCmdType()
	return cmdtype == raft_cmdpb.CmdType_Snap || cmdtype == raft_cmdpb.CmdType_Get
}

//如果有一条是read,那么全是read.
//func (d *peerMsgHandler) processReadRequest(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) bool {
//	var resps = make([]raft_cmdpb.Response, len(msg.GetRequests()))
//	for idx, req := range msg.GetRequests() {
//		cmd := req.GetCmdType()
//		resps[idx].CmdType = cmd
//		switch cmd {
//		case raft_cmdpb.CmdType_Get:
//			getReq := req.GetGet()
//			var value []byte
//			err := util.CheckKeyInRegion(getReq.GetKey(), d.Region())
//			if err == nil {
//				value, err = engine_util.GetCF(d.ctx.engine.Kv, getReq.GetCf(), getReq.GetKey())
//			}
//			if err != nil {
//				log.Errorf("GetCF error:%s", err.Error())
//			} else {
//				resps[idx].Get = &raft_cmdpb.GetResponse{Value: value}
//				util.RSDebugf("%s processReadRequest GetCF(%s,%s)='%s'", d.Tag, getReq.GetCf(), string(getReq.GetKey()), string(value))
//			}
//		case raft_cmdpb.CmdType_Snap:
//			resps[idx].Snap = &raft_cmdpb.SnapResponse{
//				Region: d.peer.Region(),
//			}
//			cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
//			util.RSDebugf("%s processReadRequest %v", d.Tag, cmd)
//		default:
//			log.Warnf("%s processReadRequest:wrong2 msg type '%v'", d.Tag, cmd)
//			return false
//		}
//	}
//	resp := newCmdResp()
//	//set header;
//	resp.Header.CurrentTerm = msg.GetHeader().GetTerm()
//	for idx := 0; idx < len(resps); idx++ {
//		resp.Responses = append(resp.Responses, &resps[idx])
//	}
//	cb.Done(resp)
//	return true
//}

func (d *peerMsgHandler) processEntry_raftMsg(rmw *RaftMsgWrapper) {
	if len(rmw.raftmsg.GetRequests()) == 0 {
		return
	}
	cb := d.peer.popCallback(rmw)
	epoch := d.Region().GetRegionEpoch()
	hdrEpoch := rmw.raftmsg.GetHeader().GetRegionEpoch()
	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if hdrEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s", d.Tag, hdrEpoch, epoch)
		rErr := &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, hdrEpoch, epoch),
			Regions: []*metapb.Region{d.Region()},
		}
		cb.Done(ErrResp(rErr))
		return
	}
	var err error

	var resps = make([]raft_cmdpb.Response, len(rmw.raftmsg.GetRequests()))
	var wb engine_util.WriteBatch
	for idx, req := range rmw.raftmsg.GetRequests() {
		resps[idx].CmdType = req.CmdType
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			if cb.Callback == nil {
				continue
			}
			getReq := req.GetGet()
			err := util.CheckKeyInRegion(getReq.GetKey(), d.Region())
			if err != nil {
				log.Errorf("%s CheckKeyInRegion error:%s", d.Tag, err.Error())
				cb.Done(ErrResp(err))
				return
			}
			value, err := engine_util.GetCF(d.ctx.engine.Kv, getReq.GetCf(), getReq.GetKey())
			if err != nil {
				log.Errorf("GetCF error:%s", err.Error())
				cb.Done(ErrResp(err))
				return
			} else {
				resps[idx].Get = &raft_cmdpb.GetResponse{Value: value}
				util.RSDebugf("%s processEntry GetCF(%s,%s)='%s'", d.Tag, getReq.GetCf(), string(getReq.GetKey()), string(value))
				//log.TestLog("%s processEntry GetCF(%s,%s)='%s'", d.Tag, getReq.GetCf(), string(getReq.GetKey()), string(value))
			}
		case raft_cmdpb.CmdType_Snap:
			if cb.Callback == nil {
				continue
			}
			//log.TestLog("%s processEntry Snapshot", d.Tag)
			resps[idx].Snap = &raft_cmdpb.SnapResponse{
				Region: d.Region(),
			}
			cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
			util.RSDebugf("%s processEntry %v", d.Tag, req.CmdType)
		case raft_cmdpb.CmdType_Put:
			put := req.GetPut()
			err := util.CheckKeyInRegion(put.GetKey(), d.Region())
			if err != nil {
				log.Errorf("%s CheckKeyInRegion error:%s", d.Tag, err.Error())
				cb.Done(ErrResp(err))
				return
			}
			wb.SetCF(put.GetCf(), put.GetKey(), put.GetValue())
			resps[idx].Put = &raft_cmdpb.PutResponse{}
			util.RSDebugf("%s SetCF(%s,%s)='%s'", d.Tag, put.GetCf(), string(put.GetKey()), string(put.GetValue()))
		case raft_cmdpb.CmdType_Delete:
			del := req.GetDelete()
			err := util.CheckKeyInRegion(del.GetKey(), d.Region())
			if err != nil {
				log.Errorf("%s CheckKeyInRegion error:%s", d.Tag, err.Error())
				cb.Done(ErrResp(err))
				return
			}
			wb.DeleteCF(del.GetCf(), del.GetKey())
			resps[idx].Delete = &raft_cmdpb.DeleteResponse{}
			util.RSDebugf("%s DeleteCF(%s,%s)", d.Tag, del.GetCf(), string(del.GetKey()))
		default:
			log.Warnf("processEntry:wrong2 msg type '%v'", req.CmdType)
		}
	}
	err = wb.WriteToDB(d.ctx.engine.Kv)
	if err != nil {
		cb.Done(ErrResp(err))
		log.Error("processEntry error:%s", err.Error())
		return
	}
	resp := newCmdResp()
	//set header;
	resp.Header.CurrentTerm = rmw.raftmsg.GetHeader().GetTerm()
	for idx := 0; idx < len(resps); idx++ {
		resp.Responses = append(resp.Responses, &resps[idx])
	}
	cb.Done(resp)
}

func (d *peerMsgHandler) processEntry(ent *eraftpb.Entry) {
	if len(ent.GetData()) == 0 {
		util.RSDebugf("processEntry %s", ent.String())
		return
	}
	var rmw RaftMsgWrapper
	err := rmw.Unmarshal(ent.Data)
	if err != nil {
		log.Warnf("processEntry Unmarshal err:%s.", err.Error())
		return
	}

	util.RSDebugf("%s processEntry:%s.", d.peer.Tag, rmw.String())
	if rmw.raftmsg.GetAdminRequest() != nil {
		d.onAdminRequest(rmw.raftmsg.GetHeader(), rmw.raftmsg.GetAdminRequest())
	}
	//apply;
	d.processEntry_raftMsg(&rmw)
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) onCompactLog(hdr *raft_cmdpb.RaftRequestHeader, msg *raft_cmdpb.CompactLogRequest) {
	compactIdx := msg.GetCompactIndex()
	compactTerm := msg.GetCompactTerm()
	//可能是落后的node，需要进行install snapshot，所以直接返回.
	psLast, err := d.peerStorage.LastIndex()
	if err != nil {
		log.Errorf("%s onCompactLog peerStorage.LastIndex() err:%s", d.Tag, err.Error())
		return
	}
	if compactIdx > psLast {
		log.Errorf(`%s onCompactLog(%d) is lost node(%d)`, d.Tag, compactIdx, psLast)
		return
	}
	//check;
	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Errorf("%s onCompactLog term(%d) err:%s", d.Tag, compactIdx, err.Error())
		return
	}
	if term != compactTerm {
		log.Errorf("%s onCompactLog term(%d->%d) != compactTerm(%d) ", d.Tag, compactIdx, term, compactTerm)
		return
	}
	psTerm, err := d.peerStorage.Term(compactIdx)
	if err != nil {
		log.Errorf("%s onCompactLog psTerm(%d) err:%s", d.Tag, compactIdx, err.Error())
		return
	}
	if psTerm != compactTerm {
		log.Errorf("%s onCompactLog psTerm(%d->%d) != compactTerm(%d)", d.Tag, compactIdx, psTerm, compactTerm)
		return
	}
	//reset truncate index;(这个是放这删除之前还是删除之后？)
	var kvwb engine_util.WriteBatch
	stor := d.peerStorage
	stor.applyState.TruncatedState.Index = compactIdx
	stor.applyState.TruncatedState.Term = compactTerm
	kvwb.SetMeta(meta.ApplyStateKey(d.regionId), stor.applyState)
	kvwb.WriteToDB(d.ctx.engine.Kv)
	//compact;
	d.ScheduleCompactLog(0, compactIdx)

}

func (d *peerMsgHandler) onAdminRequest(hdr *raft_cmdpb.RaftRequestHeader, msg *raft_cmdpb.AdminRequest) {
	switch msg.GetCmdType() {
	case raft_cmdpb.AdminCmdType_CompactLog:
		d.onCompactLog(hdr, msg.GetCompactLog())
	case raft_cmdpb.AdminCmdType_Split:
		d.onRegionSplit(hdr, msg.GetSplit())
	default:
		log.Warnf("'%v' was not support.", msg.GetCmdType())
	}
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		//log.Errorf("%s proposeRaftCommand err:%s", d.Tag, err.Error())
		cb.Done(ErrResp(err))
		return
	}
	//Your Code Here (2B).
	if true == d.processNoreplicate(msg, cb) {
		return
	}
	d.raftPropose(msg, cb)
}

func (d *peerMsgHandler) processNoreplicate(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) bool {
	//TransferLeader actually is an action no need to replicate to other peers
	if msg.GetAdminRequest() != nil {
		admin := msg.GetAdminRequest()
		switch admin.GetCmdType() {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			d.proposeRaftCommandTransferLeader(msg.GetAdminRequest(), cb)
			return true
		case raft_cmdpb.AdminCmdType_ChangePeer:
			d.proposeRaftCommandChangePeer(msg.GetAdminRequest(), cb)
			return true
		}
	}
	return false
}

var ccIndex uint64

func adminConfChangeResp(cmd raft_cmdpb.AdminCmdType, cb *message.Callback, desc string) {
	if cb != nil {
		if cmd == raft_cmdpb.AdminCmdType_ChangePeer {
			log.Debugf(`%s and resp`, desc)
		}
		resp := newCmdResp()
		resp.AdminResponse = &raft_cmdpb.AdminResponse{
			CmdType: cmd,
		}
		switch cmd {
		case raft_cmdpb.AdminCmdType_ChangePeer:
			resp.AdminResponse.ChangePeer = &raft_cmdpb.ChangePeerResponse{}
		case raft_cmdpb.AdminCmdType_TransferLeader:
			resp.AdminResponse.TransferLeader = &raft_cmdpb.TransferLeaderResponse{}
		}
		cb.Done(resp)
	} else {
		log.Debugf(desc)
	}
}

func (d *peerMsgHandler) proposeRaftCommandChangePeer(admin *raft_cmdpb.AdminRequest, cb *message.Callback) {
	if d.RaftGroup.Raft.PendingConfIndex > 0 {
		//NOTICE-3B:单节点变更：一次只能有一个confChange进行中.
		str := fmt.Sprintf(`%s %d proposeRaftCommandChangePeer err:ConfChange was doing.should commit latter.`,
			d.Tag, d.Meta.GetId())
		adminConfChangeResp(admin.GetCmdType(), cb, str)
		return
	}
	req := admin.GetChangePeer()
	removPeer := req.GetPeer()
	self := d.Meta
	//NOTICE-3B:要删除自己（是leader），那么需要先transfer,返回返回。至于删除，就留到下一任去做吧.
	if req.GetChangeType() == eraftpb.ConfChangeType_RemoveNode {
		if removPeer.GetId() == self.GetId() {
			//选择日志最接近自己到follower.
			peer := d.latestFollower()
			if peer == nil {
				log.Errorf("%s no peer to transfer", d.Tag)
				return
			}
			d.RaftGroup.TransferLeader(peer.GetId())
			str := fmt.Sprintf(`%s %d proposeRaftCommandChangePeer,remove leader self .should first transfer leader to(%s) and return `,
				d.Tag, d.Meta.GetId(), peer)
			adminConfChangeResp(admin.GetCmdType(), cb, str)
			return
		}
	}
	//NOTICE-3B:要将数据带过去（leader带到整个集群，在apply到时候要用到storeid）
	var tmp = &ConfChange{Peer: req.GetPeer()}
	data, err := tmp.Marshal()
	if err != nil {
		str := fmt.Sprintf(`%s %d proposeRaftCommandChangePeer %s err:%s`,
			d.Tag, d.Meta.GetId(), tmp.String(), err.Error())
		adminConfChangeResp(admin.GetCmdType(), cb, str)
		return
	}

	cc := eraftpb.ConfChange{
		ChangeType: req.GetChangeType(),
		NodeId:     req.GetPeer().GetId(),
		Context:    data,
	}
	d.RaftGroup.ProposeConfChange(cc)
	//d.peer.insertPeerCache(req.GetPeer())
	adminConfChangeResp(admin.GetCmdType(), cb, fmt.Sprintf(`%s %d proposeRaftCommandChangePeer(%v,%d_%d)`, d.Tag, d.Meta.GetId(), req.GetChangeType(), req.GetPeer().GetId(), ccIndex))
}

func (d *peerMsgHandler) proposeRaftCommandTransferLeader(admin *raft_cmdpb.AdminRequest, cb *message.Callback) {
	req := admin.GetTransferLeader()
	d.RaftGroup.TransferLeader(req.GetPeer().GetId())
	//d.peer.removePeerCache(req.GetPeer().GetId())
	adminConfChangeResp(admin.GetCmdType(), cb, fmt.Sprintf(`%s proposeRaftCommandTransferLeader(%d)`, d.Tag, req.GetPeer().GetId()))
}

func (d *peerMsgHandler) addNode(region *metapb.Region, cc *eraftpb.ConfChange) {
	nodeId := cc.GetNodeId()
	tmp := new(ConfChange)
	err := tmp.Unmarshal(cc.Context)
	if err != nil {
		log.Errorf("%s addNode -%d unmarshal err:%s", d.Tag, nodeId, err.Error())
		return
	}
	nodeTag := fmt.Sprintf("%d-%d", nodeId, tmp.Index)
	//has add to peer.cache;
	peer := tmp.Peer

	//region 已经存在，那么直接跳过.
	if util.FindPeer(region, peer.GetStoreId()) != nil {
		log.Warnf("%s has added node(%s) before.", d.Tag, nodeTag)
		//need do nothing;
		return
	}
	//set region
	region.Peers = append(region.Peers, peer)
	region.GetRegionEpoch().ConfVer++
	//save to db;
	var kvwb engine_util.WriteBatch
	meta.WriteRegionState(&kvwb, region, rspb.PeerState_Normal)
	err = kvwb.WriteToDB(d.peerStorage.Engines.Kv)
	if err != nil {
		log.Errorf("%s AddNode(%s)writeToDB err:%s", d.Tag, nodeTag, err.Error())
		return
	}
	//NOTICE-3B:修改具体的配置，放到落库成功——万一失败，那么就完全不需要回滚，直接就是原来到配置.
	d.insertPeerCache(peer)
	d.SetRegion(region)
	log.Errorf("%s add(%s)", d.Tag, nodeTag)
}

func (d *peerMsgHandler) removeNode(region *metapb.Region, cc *eraftpb.ConfChange) {
	nodeId := cc.GetNodeId()
	tmp := new(ConfChange)
	err := tmp.Unmarshal(cc.Context)
	if err != nil {
		log.Errorf("%s removeNode-%d unmarshal err:%s", d.Tag, nodeId, err.Error())
		return
	}
	nodeTag := fmt.Sprintf("%d-%d", nodeId, tmp.Index)
	//find peer;
	peer := d.peer.getPeerFromCache(nodeId)
	if nil == util.RemovePeer(region, peer.GetStoreId()) {
		log.Infof("%s has removed node(%s) before.", d.Tag, nodeTag)
		//need do nothing;
		return
	}
	//set region
	if d.Meta.GetId() != nodeId {
		region.GetRegionEpoch().ConfVer++
		util.RemovePeer(region, peer.GetStoreId())
	} else {
		//clear peers;
		//region.Peers = []*metapb.Peer{}
		//NOTICE-3B:这里不需要置空，因为这里destroy之后，会重新new，用到是新到数据，这里有啥都无所谓来.
	}
	//save to db;
	if d.Meta.GetId() == nodeId {
		d.SetRegion(region)
		d.destroyPeer() //save db inner;
		log.Errorf("%s remove(%s) and destroyPeer", d.Tag, nodeTag)
		return
	} else {
		//NOTICE-3B:destory 中会WriteRegionState.如果不调用destory，则需要手动调用save下.
		var kvwb engine_util.WriteBatch
		meta.WriteRegionState(&kvwb, region, rspb.PeerState_Normal)
		err = kvwb.WriteToDB(d.peerStorage.Engines.Kv)
		if err != nil {
			log.Errorf("%s AddNode(%s)writeToDB err:%s", d.Tag, nodeTag, err.Error())
		} else {
			d.SetRegion(region)
		}
	}
	log.Errorf("%s remove(%s)", d.Tag, nodeTag)
}

func (d *peerMsgHandler) updateConfChange(cc *eraftpb.ConfChange) {
	//
	region := new(metapb.Region)
	err := util.CloneMsg(d.Region(), region)
	if err != nil {
		log.Warnf("%s updateConfChange err:%s", d.Tag, err.Error())
		return
	}
	//预处理;
	switch cc.GetChangeType() {
	case eraftpb.ConfChangeType_AddNode:
		d.addNode(region, cc)
	case eraftpb.ConfChangeType_RemoveNode:
		d.removeNode(region, cc)
	default:
		log.Warnf("%s unknown ConfChangeType(%v)", d.Tag, cc.GetChangeType())
		return
	}
}

// --------
// NOTICE-3B-split:
//
//	1、先创建new-region（peer设置好，但是没有keys）.
//	2、根据peer来创建peer.
//	3、分割keys: new/old region split keys
func checkSplitKey(split, start, end []byte) bool {
	//需要start < split < end ，否则split没有意义.
	ret1 := false //start < split;
	if len(start) == 0 {
		ret1 = true
	} else { //len(start) > 0
		ret1 = (bytes.Compare(start, split) < 0)
	}
	if false == ret1 {
		return false
	}
	//split < end
	if len(end) == 0 {
		return true
	}
	return bytes.Compare(split, end) < 0
}
func (d *peerMsgHandler) onRegionSplit(hdr *raft_cmdpb.RaftRequestHeader, req *raft_cmdpb.SplitRequest) {

	region := new(metapb.Region)
	err := util.CloneMsg(d.Region(), region)
	if err != nil {
		log.Warnf("%s onRegionSplit err:%s", d.Tag, err.Error())
		return
	}
	if !checkSplitKey(req.GetSplitKey(), region.GetStartKey(), region.GetEndKey()) {
		//[start<end)<=splitKey;可能会触发多次split.
		//log.Warnf("%s onRegionSplit start(%x) end(%x) split(%x)req=%s", d.Tag, region.GetStartKey(), region.GetEndKey(), req.GetSplitKey(), req)
		return
	}
	log.Infof("%s onRegionSplit %s ", d.Tag, req)

	if len(region.GetPeers()) != len(req.GetNewPeerIds()) {
		log.Errorf("%s len(region.GetPeers())<%d> != len(req.GetNewPeerIds())<%d>", d.Tag, len(region.GetPeers()), len(req.GetNewPeerIds()))
		return
	}
	newRegion := d.CreateRegion(region, req)
	if newRegion == nil {
		return
	}
	ctx := d.ctx
	newPeer, err := createPeer(d.Meta.StoreId, ctx.cfg, ctx.regionTaskSender, d.peerStorage.Engines, newRegion)
	if err != nil {
		log.Error("%d CreatePeer(%+v)error:%s", d.Meta.StoreId, region, err.Error())
		return
	}
	ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion})
	ctx.storeMeta.regions[newRegion.GetId()] = newRegion
	ctx.router.register(newPeer)
	//trigger start;
	err = ctx.router.send(newRegion.GetId(), message.Msg{RegionID: newRegion.GetId(), Type: message.MsgTypeStart})
	if err != nil {
		log.Errorf("%s start failed:%s", newPeer.Tag, err.Error())
		return
	}
	//reset origin region;
	ctx.storeMeta.regionRanges.ReplaceOrInsert(&regionItem{region: region})
	ctx.storeMeta.regions[region.GetId()] = region
	d.SetRegion(region)
	//
	log.TestLog("%s onRegionSplit(%s-%d) ok. {%d:'%s'->'%s'},{%d:'%s'->'%s';%+v} ",
		d.Tag, newPeer.Tag, newPeer.Meta.StoreId,
		region.GetId(), region.GetStartKey(), region.GetEndKey(),
		newRegion.GetId(), newRegion.GetStartKey(), newRegion.GetEndKey(), req.GetNewPeerIds())
}

func (d *peerMsgHandler) CreateRegion(region *metapb.Region, req *raft_cmdpb.SplitRequest) *metapb.Region {
	myStoreId := d.Meta.GetStoreId()
	//set peers;
	sortPeers := PeerSlice(region.GetPeers())
	sort.Sort(sortPeers)
	npeers := req.GetNewPeerIds()
	sortkeys.Uint64s(npeers)
	var newPeer *metapb.Peer
	newPeerList := make([]*metapb.Peer, len(sortPeers))
	for idx, p := range sortPeers {
		newPeerList[idx] = &metapb.Peer{
			Id:      npeers[idx],
			StoreId: p.GetStoreId(),
		}
		if p.GetStoreId() == myStoreId {
			newPeer = newPeerList[idx]
		}
	}
	if newPeer == nil {
		log.Errorf("%s CreateRegion not found %d in region=%+v", d.Tag, myStoreId, region)
		return nil
	}
	engines := d.peerStorage.Engines
	//init;
	newRegion := &metapb.Region{
		Id:       req.GetNewRegionId(),
		StartKey: []byte{},
		EndKey:   []byte{},
		RegionEpoch: &metapb.RegionEpoch{
			Version: InitEpochVer,
			ConfVer: InitEpochConfVer,
		},
		Peers: newPeerList,
	}
	var kvwb engine_util.WriteBatch
	newRegion.StartKey = req.GetSplitKey()
	newRegion.EndKey = region.GetEndKey()
	writeInitialApplyState(&kvwb, region.Id)
	meta.WriteRegionState(&kvwb, newRegion, rspb.PeerState_Normal)
	err := engines.WriteKV(&kvwb)
	if err != nil {
		log.Errorf("%s onRegionSplit new-%d writeToKV err:%s", d.Tag, newRegion.GetId(), err.Error())
		return nil
	}

	raftWB := new(engine_util.WriteBatch)
	writeInitialRaftState(raftWB, region.Id)
	err = engines.WriteRaft(raftWB)
	if err != nil {
		log.Errorf("%s onRegionSplit new-%d writeToRaft err:%s", d.Tag, newRegion.GetId(), err.Error())
		return nil
	}
	//reset region;
	region.EndKey = req.GetSplitKey()
	region.RegionEpoch.Version++
	//save to db;
	kvwb.Reset()
	meta.WriteRegionState(&kvwb, region, rspb.PeerState_Normal)
	err = engines.WriteKV(&kvwb)
	if err != nil {
		log.Errorf("%s onRegionSplit writeToDB err:%s", d.Tag, err.Error())
		return nil
	}
	return newRegion
}

type PeerSlice []*metapb.Peer

func (ps PeerSlice) Less(i, j int) bool {
	ips, jps := ps[i], ps[j]
	if ips.GetId() < jps.GetStoreId() {
		return true
	}
	if ips.GetId() == jps.GetId() {
		return ips.GetStoreId() < jps.GetStoreId()
	}
	return false
}

func (ps PeerSlice) Swap(i, j int) {
	ps[i], ps[j] = ps[j], ps[i]
}

func (ps PeerSlice) Len() int {
	return len(ps)
}
