package server

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/log"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
	"io"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).
// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	var resp kvrpcpb.RawGetResponse
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("RawGet error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		resp.Error = err.Error()
		return &resp, nil
	}
	defer reader.Close()
	//get ;
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		log.Errorf("RawGet error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		resp.Error = err.Error()
		return &resp, nil
	}
	log.TestLog("RawGet %s=%s", string(req.GetKey()), string(value))

	resp.Value = value
	if len(value) == 0 {
		resp.NotFound = true
	}
	return &resp, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	var resp kvrpcpb.RawPutResponse
	// Your Code Here (1).
	var modify = storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}

	err := server.storage.Write(req.GetContext(), []storage.Modify{{modify}})
	if err != nil {
		log.Errorf("RawPut error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		resp.Error = err.Error()
		return &resp, nil
	}
	return &resp, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	var resp kvrpcpb.RawDeleteResponse
	// Your Code Here (1).
	modify := storage.Delete{Key: req.GetKey(), Cf: req.GetCf()}
	err := server.storage.Write(req.GetContext(), []storage.Modify{{modify}})
	if err != nil {
		log.Errorf("RawDelete error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		resp.Error = err.Error()
		return &resp, nil
	}
	return &resp, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	var resp kvrpcpb.RawScanResponse
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("RawScan error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		resp.Error = err.Error()
		return &resp, nil
	}
	defer reader.Close()
	itr := reader.IterCF(req.GetCf())
	defer itr.Close()
	itr.Seek(req.GetStartKey())
	//get ;
	for itr.Valid() && req.Limit > 0 {
		item := itr.Item()
		var kv = new(kvrpcpb.KvPair)
		kv.Key = item.Key()
		kv.Value, _ = item.Value()
		resp.Kvs = append(resp.Kvs, kv)
		//next;
		itr.Next()
		req.Limit--
	}
	return &resp, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	log.Debugf("KvGet(%x,ver=%d)", req.GetKey(), req.GetVersion())
	// Your Code Here (4B).
	var resp kvrpcpb.GetResponse
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvGet error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		log.Errorf("KvGet error:%s", err.Error())
		//TODO: changed later;
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	if lock != nil {
		//TODO: lcoker ttl how to do???
		if lock.IsLockedFor(req.GetKey(), txn.StartTS, &resp) {
			log.Debugf("KvGet was locked:%+v", lock.Info(req.GetKey()))
			return &resp, nil
		} else {
			log.Debugf("KvGet was not locked:%+v", lock.Info(req.GetKey()))
		}
	}
	//do lock;
	value, err := txn.GetValue(req.GetKey())
	if err != nil {
		log.Errorf("KvGet error:%s", err.Error())
		//TODO: changed later;
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	if len(value) == 0 {
		resp.NotFound = true
	} else {
		resp.Value = value
	}
	return &resp, nil
}

//KvPrewrite is where a value is actually written to the database.
//	A key is locked and a value stored. We must check that another transaction has not locked or written to the same key.
type TmpLock struct {
	Error *kvrpcpb.KeyError `protobuf:"bytes,2,opt,name=error" json:"error,omitempty"`
}

func lockTimeOut(lock *mvcc.Lock, txn *mvcc.MvccTxn) bool {
	return lock.Ttl+lock.Ts < txn.StartTS
}

func (Server *Server) rollBack() {
	log.Errorf("rollBack was not support")
}

func checkPreWriteLock(txn *mvcc.MvccTxn, key, primary []byte) (ok bool, region *errorpb.Error, keyErr *kvrpcpb.KeyError) {
	lock, err := txn.GetLock(key)
	if err != nil {
		//TODO: changed later;
		return false, util.RaftstoreErrToPbError(err), nil
	}
	if lock != nil {
		var tl TmpLock
		if lock.IsLockedFor(primary, txn.StartTS, &tl) {
			return false, nil, tl.Error
		} else {
			//not lock;
		}
	}
	return true, nil, nil
}

func checkPreWriteWrite(txn *mvcc.MvccTxn, key, primary []byte) (ok bool, region *errorpb.Error, keyErr *kvrpcpb.KeyError) {
	w, ts, err := txn.MostRecentWrite(key)
	if err != nil {
		return false, util.RaftstoreErrToPbError(err), nil
	}
	if w != nil &&
		(w.StartTS <= txn.StartTS && txn.StartTS <= ts) {
		return false, nil, &kvrpcpb.KeyError{
			Conflict: &kvrpcpb.WriteConflict{
				StartTs:    w.StartTS,
				ConflictTs: ts,
				Key:        key,
				Primary:    primary,
			},
		}
	}
	return true, nil, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	log.Debugf("KvPrewrite(sv=%d,%+v)", req.GetStartVersion(), req.GetMutations())
	//
	var resp kvrpcpb.PrewriteResponse
	//for check;
	if len(req.GetMutations()) == 0 {
		return &resp, nil
	}
	//create txn;
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvPrewrite error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	//check locker;
	// 1- primary key;
	primaryLock := &mvcc.Lock{
		Primary: req.GetPrimaryLock(),
		Ts:      req.GetStartVersion(),
		Ttl:     req.GetLockTtl(),
	}
	// 2- all keys;
	for _, m := range req.GetMutations() {
		ok, regionErr, keyErr := checkPreWriteLock(txn, m.GetKey(), req.GetPrimaryLock())
		if !ok {
			log.Warnf("KvPrewrite checkLock failed:%v;%v.", regionErr, keyErr)
			if regionErr != nil {
				resp.RegionError = regionErr
				return &resp, nil
			} else {
				resp.Errors = append(resp.Errors, keyErr)
				continue
			}
		}
		ok, regionErr, keyErr = checkPreWriteWrite(txn, m.GetKey(), req.GetPrimaryLock())
		if !ok {
			log.Warnf("KvPrewrite checkWrite failed:%v;%v.", regionErr, keyErr)
			if regionErr != nil {
				resp.RegionError = regionErr
				return &resp, nil
			} else {
				resp.Errors = append(resp.Errors, keyErr)
				continue
			}
		}

		//这里的数据仅仅在内存中，还没写文件，所以没关系.
		primaryLock.Kind = mvcc.WriteKindFromProto(m.GetOp())
		txn.PutLock(m.GetKey(), primaryLock)
		switch m.GetOp() {
		case kvrpcpb.Op_Put:
			txn.PutValue(m.GetKey(), m.GetValue())
		case kvrpcpb.Op_Del:
			txn.DeleteValue(m.GetKey())
		}
	}
	if len(resp.Errors) > 0 {
		//如果已经有失败了，那么就没有必要lock了.
		return &resp, nil
	}
	//flush storage;
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		log.Errorf("KvPrewrite error:%s", err.Error())
		//TODO: changed later;
		resp.RegionError = util.RaftstoreErrToPbError(err)
		server.rollBack()
		return &resp, nil
	}
	return &resp, nil
}

//KvCommit does not change the value in the database, but it does record that the value is committed.
//KvCommit will fail if the key is not locked or is locked by another transaction.

// Commit is the second phase of 2pc. The client must have successfully prewritten
// the transaction to all nodes. If all keys are locked by the given transaction,
// then the commit should succeed. If any keys are locked by a different
// transaction or are not locked at all (rolled back or expired), the commit
// fails.
func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	log.Debugf("KvCommit(sv=%d,cv=%d,%+v)", req.GetStartVersion(), req.GetCommitVersion(), req.GetKeys())
	// Your Code Here (4B).
	var resp kvrpcpb.CommitResponse
	//create txn;
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvCommit error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetCommitVersion())
	//check lock;
	var tmp TmpLock
	for _, key := range req.GetKeys() {
		if !doCommit(txn, req.GetStartVersion(), req.GetCommitVersion(), key, &tmp) {
			break
		}
	}
	if tmp.Error != nil {
		//如果已经有失败了，那么就没有必要lock了.
		resp.Error = tmp.Error
		return &resp, nil
	}
	//flush storage;
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		log.Errorf("KvPrewrite error:%s", err.Error())
		//TODO: changed later;
		resp.RegionError = util.RaftstoreErrToPbError(err)
		server.rollBack()
		return &resp, nil
	}
	return &resp, nil
}

func doCommit(txn *mvcc.MvccTxn, startTs, commitTs uint64, key []byte, resp *TmpLock) bool {
	//
	wr, wts, err := txn.MostRecentWrite(key)
	if err != nil {
		log.Errorf("KvCommit error:%s", err.Error())
		resp.Error = &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
		return false
	}
	if wr != nil {
		if wr.StartTS == startTs && wts == commitTs {
			//幂等性.(重复commit)
			return false
		}
		if wts > commitTs {
			log.Fatalf("KvCommit has find newest commit:%+v,wts=%d", wr, wts)
		}
	}
	//
	lock, err := txn.GetLock(key)
	if err != nil {
		log.Errorf("KvCommit(%x) error:%s", key, err.Error())
		//TODO: changed later;
		resp.Error = &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
		return false
	}
	if lock == nil {
		//no locker;
		//resp.Error = &kvrpcpb.KeyError{Abort: "there is no locker.maybe expired."}
		log.Warnf("there is no locker.maybe expired.")
		return false
	}
	//
	if startTs != lock.Ts {
		resp.Error = &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
			StartTs:    lock.Ts,
			ConflictTs: startTs,
			Key:        key,
			Primary:    lock.Primary,
		}}
		return false
	}
	if bytes.Equal(lock.Primary, key) {
		log.Debugf("KvCommit this is primary")
	}
	txn.PutWrite(key, commitTs, &mvcc.Write{
		StartTS: startTs,
		Kind:    lock.Kind,
	})
	txn.DeleteLock(key)
	return true
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	log.Debugf("KvScan(sv=%d,(%d)0x%x)", req.GetVersion(), req.GetLimit(), req.GetStartKey())
	//
	var resp kvrpcpb.ScanResponse
	if req.GetLimit() == 0 {
		log.Infof("KvScan(limit=0,0x%x)", req.GetStartKey())
		return &resp, nil
	}
	//create txn;
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvScan error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetVersion())
	scanner := mvcc.NewScanner(req.GetStartKey(), txn)
	defer scanner.Close()
	//set limit;
	limit := req.GetLimit()
	//if limit == 0 {
	//	limit = math.MaxUint32
	//}
	//get value;
	for i := uint32(0); i < limit; i++ {
		key, value, err := scanner.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Warnf("KvScan err:%s", err.Error())
			continue
		}
		resp.Pairs = append(resp.Pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
		log.Debugf("KvScan '%x'='%x'", key, value)
	}
	return &resp, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	log.Debugf("KvCheckTxnStatus(sv=%d,curTs=%x,0x%x)", req.GetLockTs(), req.GetCurrentTs(), req.GetPrimaryKey())
	//
	var resp kvrpcpb.CheckTxnStatusResponse
	//create txn;
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvCheckTxnStatus error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()

	commitTxn := mvcc.NewMvccTxn(reader, req.GetLockTs())
	//1-check write;
	write, writeTS, err := commitTxn.RecentWrite(req.GetPrimaryKey())
	if err != nil {
		log.Errorf("KvCheckTxnStatus.GetLock(0x%x) err:%s", req.GetPrimaryKey(), err.Error())
		return nil, err
	}

	if write != nil { //do nothing;
		log.TxnDbg("KvCheckTxnStatus has commit(%v)", write.Kind)
		resp.Action = kvrpcpb.Action_NoAction
		if write.Kind != mvcc.WriteKindRollback {
			resp.CommitVersion = writeTS
		}
		return &resp, nil
	}
	//2-check locker;
	lock, err := commitTxn.GetLock(req.GetPrimaryKey())
	if err != nil {
		log.Errorf("KvCheckTxnStatus.GetLock(0x%x) err:%s", req.GetPrimaryKey(), err.Error())
		return nil, err
	}
	//lock is expired;
	var modifyBuf []storage.Modify
	if lock != nil {
		resp.LockTtl = lock.Ttl
		if lock.IsExpired(req.GetCurrentTs()) {
			log.TxnDbg("KvCheckTxnStatus has expired")
			write := mvcc.Write{
				lock.Ts,
				mvcc.WriteKindRollback,
			}
			commitTxn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &write)
			commitTxn.DeleteLock(req.GetPrimaryKey())
			modifyBuf = commitTxn.Writes()
			//
			defaultTxn := mvcc.NewMvccTxn(reader, lock.Ts)
			defaultTxn.DeleteValue(req.GetPrimaryKey())
			modifyBuf = append(modifyBuf, defaultTxn.Writes()...)
			//set resp;
			resp.Action = kvrpcpb.Action_TTLExpireRollback
			resp.CommitVersion = req.GetCurrentTs()
		} else {
			//not expire,do noting;
			resp.Action = kvrpcpb.Action_NoAction
		}
	} else {
		//no locker;
		write := mvcc.Write{
			req.GetLockTs(),
			mvcc.WriteKindRollback,
		}
		commitTxn.PutWrite(req.GetPrimaryKey(), req.GetLockTs(), &write)
		modifyBuf = commitTxn.Writes()
		//set resp;
		resp.Action = kvrpcpb.Action_LockNotExistRollback
	}

	//flush storage;
	err = server.storage.Write(req.GetContext(), modifyBuf)
	if err != nil {
		log.Errorf("KvCheckTxnStatus error:%s", err.Error())
		//TODO: changed later;
		resp.RegionError = util.RaftstoreErrToPbError(err)
		server.rollBack()
		return &resp, nil
	}
	return &resp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	log.Debugf("KvBatchRollback(sv=%d,%d)", req.GetStartVersion(), len(req.GetKeys()))
	//
	var resp kvrpcpb.BatchRollbackResponse
	//create txn;
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvBatchRollback error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	var tmp TmpLock
	for _, key := range req.GetKeys() {
		if !doRollback(txn, req.GetStartVersion(), key, &tmp) {
			break
		}
	}
	if tmp.Error != nil {
		//如果已经有失败了，那么就没有必要lock了.
		resp.Error = tmp.Error
		return &resp, nil
	}
	//flush storage;
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		log.Errorf("KvBatchRollback error:%s", err.Error())
		//TODO: changed later;
		resp.RegionError = util.RaftstoreErrToPbError(err)
		server.rollBack()
		return &resp, nil
	}
	return &resp, nil
}

func doRollback(txn *mvcc.MvccTxn, startTs uint64, key []byte, resp *TmpLock) bool {
	write, _, err := txn.CurrentWrite(key)
	if err != nil {
		log.Warnf("KvBatchRollback.KvBatchRollback(0x%x) err:%s", key, err.Error())
		resp.Error = &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
		return false
	}
	//1.has commit;
	if write != nil {
		//1.1-has commit,can not rollback.
		if write.Kind != mvcc.WriteKindRollback {
			resp.Error = &kvrpcpb.KeyError{
				Abort: fmt.Sprintf("KvBatchRollback.has commit(%v)(0x%x)", write.Kind, key),
			}
			log.Warnf(resp.Error.Abort)
			return false
		} else { //1.2-has rollback,continue
			return true
		}
	}
	//2-no write;check lock;
	lock, err := txn.GetLock(key)
	if err != nil {
		log.Warnf("KvBatchRollback.GetLock(0x%x) err:%s", key, err.Error())
		resp.Error = &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
		return false
	}
	if lock != nil {

		if lock.Ts == txn.StartTS { //2.1.1-same txn,delete it;
			txn.DeleteLock(key)
		} else { //2.1.2-not this txn locker;skip this locker;
		}
	} else { //2.2-no lock;
	}
	//3-delete default(if not exist default,delete is ok too);
	txn.DeleteValue(key)
	//4-do write;
	write = &mvcc.Write{
		startTs,
		mvcc.WriteKindRollback,
	}
	txn.PutWrite(key, startTs, write)
	return true
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	log.Debugf("KvResolveLock(sv=%d,cv=%d)", req.GetStartVersion(), req.GetCommitVersion())
	//
	var resp kvrpcpb.ResolveLockResponse
	//create txn;
	reader, err := server.storage.Reader(req.GetContext())
	if err != nil {
		log.Errorf("KvResolveLock error:%s", err.Error())
		resp.RegionError = util.RaftstoreErrToPbError(err)
		return &resp, nil
	}
	defer reader.Close()
	txn := mvcc.NewMvccTxn(reader, req.GetStartVersion())
	//
	locks, err := mvcc.AllLocksForTxn(txn)
	if err != nil {
		log.Warnf("KvResolveLock.AllLocks %s", err.Error())
		resp.Error = &kvrpcpb.KeyError{
			Abort: err.Error(),
		}
		return &resp, nil
	}
	var tmp TmpLock
	for _, lock := range locks {
		if req.CommitVersion == 0 { //rollback;
			doRollback(txn, req.GetStartVersion(), lock.Key, &tmp)
		} else {
			doCommit(txn, req.GetStartVersion(), req.GetCommitVersion(), lock.Key, &tmp)
		}
		if tmp.Error != nil {
			resp.Error = tmp.Error
			return &resp, nil
		}
	}
	err = server.storage.Write(req.GetContext(), txn.Writes())
	if err != nil {
		log.Errorf("KvResolveLock error:%s", err.Error())
		//TODO: changed later;
		resp.RegionError = util.RaftstoreErrToPbError(err)
		server.rollBack()
		return &resp, nil
	}
	return &resp, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
