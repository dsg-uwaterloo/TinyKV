package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"io"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	itr    engine_util.DBIterator
	txn    *MvccTxn
	curKey []byte
	//中间状态保存.
	scanStep scanStep
	write    *Write
}

type scanStep int

const (
	ss_roundInit scanStep = iota //ss_findNextKey
	ss_seekPos
	ss_fitWrite
	ss_getValue
	ss_eof
)

func (ss scanStep) String() string {
	switch ss {
	case ss_roundInit:
		return "roundInit"
	case ss_seekPos:
		return "seekPos"
	case ss_fitWrite:
		return "fitWrite"
	case ss_getValue:
		return "getValue"
	case ss_eof:
		return "eof"
	}
	return "unknown"
}

func (ss *scanStep) next() {
	switch *ss {
	case ss_roundInit:
		*ss = ss_seekPos
	case ss_seekPos:
		*ss = ss_fitWrite
	case ss_fitWrite:
		*ss = ss_getValue
	case ss_getValue:
		*ss = ss_roundInit
	case ss_eof: //none;
	}
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	log.Debugf("NewScanner(%x,%d)", startKey, txn.StartTS)
	// Your Code Here (4A).
	itr := txn.Reader.IterCF(engine_util.CfWrite)
	return &Scanner{
		itr,
		txn,
		startKey,
		ss_seekPos,
		nil,
	}
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.itr.Close()
}

func (scan *Scanner) seekKey() {
	scan.itr.Seek(EncodeKey(scan.curKey, scan.txn.StartTS))
	if scan.itr.Valid() {
		scan.scanStep = ss_fitWrite
		return
	}
	scan.scanStep = ss_eof
}

func (scan *Scanner) Next() (rkey []byte, rvalue []byte, rerr error) {
	// Your Code Here (4C).
	loop := true
	for loop {
		log.Debugf("Next--->(step=%v;curKey=0x%x;%d)", scan.scanStep, scan.curKey, scan.txn.StartTS)
		switch scan.scanStep {
		case ss_roundInit:
			scan.findNextKey()
		case ss_seekPos:
			scan.seekKey()
		case ss_fitWrite:
			scan.fitFitWrite()
		case ss_getValue:
			rkey, rvalue, rerr = scan.getValue()
			if rerr == nil { //find the key/value ,break loop
				loop = false
			}
			break
		case ss_eof: //no more data;
			return nil, nil, io.EOF
		}
	}
	log.Debugf("Next key=0x%x;value=0x%x", rkey, rvalue)
	return rkey, rvalue, rerr
}

func (scan *Scanner) getValue() (rkey []byte, rvalue []byte, rerr error) {
	write := scan.write
	value, err := scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(scan.curKey, write.StartTS))
	if err != nil {
		log.Warnf("GetCF(0x%x) err:%s", EncodeKey(scan.curKey, write.StartTS), err.Error())
		//如果这里发生错误,那么version更小的版本，没有意义。所以，直接nextkey（即开始新的round)
		scan.scanStep = ss_roundInit
		return nil, nil, err
	}
	rkey = scan.curKey
	rvalue = value
	scan.scanStep = ss_roundInit
	return
}

type findValueRet int

const (
	ok findValueRet = iota
	diffKey
	errVersion
	exception
)

func (scan *Scanner) findNextKey() {
	scan.scanStep = ss_seekPos
	itr := scan.itr
	curKey := scan.curKey
	//find the lastKey pos;
	lastKey := EncodeKey(curKey, 0)
	itr.Seek(lastKey)
	if false == itr.Valid() {
		scan.scanStep = ss_eof
		return
	}
	item := itr.Item()
	rawKey := DecodeUserKey(item.Key())
	//如果lastKey不存在，那么就会找到下一个key的位置，那么就直接返回;
	if false == bytes.Equal(curKey, rawKey) {
		scan.curKey = rawKey
		return
	}
	//如果lastkey存在，那么就next，看下一个key.
	itr.Next()
	if false == itr.Valid() {
		scan.scanStep = ss_eof
		return
	}
	scan.curKey = DecodeUserKey(item.Key())
}

//只是查找curKey的合适的write.
func (scan *Scanner) fitFitWrite() {
	itr := scan.itr
	curKey := scan.curKey
	for {
		write, newKey, ret := checkWrite(itr.Item(), curKey, scan.txn.StartTS)
		switch ret {
		case diffKey: //this is next key,return ;
			scan.scanStep = ss_seekPos
			scan.curKey = newKey
			return
		case errVersion: //no fit version,skip to next key;
			scan.scanStep = ss_roundInit
			return
		case exception: //go to next key;
			scan.scanStep = ss_roundInit
			return
		case ok:
		}
		switch write.Kind {
		case WriteKindDelete: //current version is no value;
			scan.scanStep = ss_roundInit //find next key;
			return
		case WriteKindRollback:
			//可能这个是不需要的.
			//itr.Next()
			//if !itr.Valid() {
			//	//理论上来说，如果rollback，必须要存在.
			//	log.Fatalf("fitFitWrite(0x%x) was no enough data", curKey)
			//	return
			//}
			itr.Next()
			if !itr.Valid() {
				scan.scanStep = ss_eof
				return
			}
			//go to find prev write key;
			continue
		case WriteKindPut:
			scan.write = write
			scan.scanStep = ss_getValue
			return
		default:
			log.Fatalf("fitFitWrite(0x%x) unknown kind(%v)", curKey, write.Kind)
		}
	}
}

func checkWrite(item engine_util.DBItem, filterKey []byte, version uint64) (*Write, []byte, findValueRet) {
	rawKey, writeVersion := decodeKey(item.Key())
	if false == bytes.Equal(rawKey, filterKey) {
		//log.Debugf("findWrite: not same key:raw[0x%x];filterKey[0x%x]", rawKey, filterKey)
		return nil, rawKey, diffKey
	}
	if version < writeVersion {
		log.Debugf("findWrite: start(%d)<writeV(%d)", version, writeVersion)
		return nil, rawKey, errVersion
	}
	//parse value(write);
	wv, err := item.Value()
	if err != nil {
		log.Warnf("findWrite: 0x%x value err:%s", rawKey, err.Error())
		return nil, rawKey, exception
	}
	write, err := ParseWrite(wv)
	if err != nil {
		log.Warnf("findWrite: 0x%x ParseWrite err:%s", rawKey, err.Error())
		return nil, rawKey, exception
	}
	return write, rawKey, ok
}
