package server

import (
	"bytes"
	"context"
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
	txn := mvcc.NewMvccTxn(reader, req.GetCommitVersion())
	//check lock;
	for _, key := range req.GetKeys() {
		//
		wr, wts, err := txn.MostRecentWrite(key)
		if err != nil {
			log.Errorf("KvCommit error:%s", err.Error())
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return &resp, nil
		}
		if wr != nil {
			if wr.StartTS == req.GetStartVersion() && wts == req.GetCommitVersion() {
				//幂等性.(重复commit)
				return &resp, nil
			}
			if wts > req.GetCommitVersion() {
				log.Fatalf("KvCommit has find newest commit:%+v,wts=%d", wr, wts)
			}
		}
		//
		lock, err := txn.GetLock(key)
		if err != nil {
			log.Errorf("KvCommit(%x) error:%s", key, err.Error())
			//TODO: changed later;
			resp.RegionError = util.RaftstoreErrToPbError(err)
			return &resp, nil
		}
		if lock == nil {
			//no locker;
			//resp.Error = &kvrpcpb.KeyError{Abort: "there is no locker.maybe expired."}
			log.Warnf("there is no locker.maybe expired.")
			return &resp, nil
		}
		//
		if req.GetStartVersion() != lock.Ts {
			resp.Error = &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs:    lock.Ts,
				ConflictTs: req.GetStartVersion(),
				Key:        key,
				Primary:    lock.Primary,
			}}
			return &resp, nil
		}
		if bytes.Equal(lock.Primary, key) {
			log.Debugf("KvCommit this is primary")
		}
		txn.PutWrite(key, req.GetCommitVersion(), &mvcc.Write{
			StartTS: req.GetStartVersion(),
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}
	if resp.Error != nil {
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

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
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
