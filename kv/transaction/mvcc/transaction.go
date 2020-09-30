package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   EncodeKey(key, ts),
		Value: write.ToBytes(),
		Cf:    engine_util.CfWrite,
	}
	log.TxnDbg("PutWrite key=%x,value=%x", key, put.Value)
	txn.writes = append(txn.writes, storage.Modify{put})
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	rv, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	if len(rv) == 0 {
		return nil, nil
	}
	return ParseLock(rv)
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   key,
		Value: lock.ToBytes(),
		Cf:    engine_util.CfLock,
	}
	log.TxnDbg("PutLock key=%x,value=%x", key, put.Value)
	txn.writes = append(txn.writes, storage.Modify{put})
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	// Your Code Here (4A).
	del := storage.Delete{
		Key: key,
		Cf:  engine_util.CfLock,
	}
	log.TxnDbg("DeleteLock key=%x,", key)
	txn.writes = append(txn.writes, storage.Modify{del})
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// Your Code Here (4A).
	//1-find the current write;
	itr := txn.Reader.IterCF(engine_util.CfWrite)
	defer itr.Close()
	itr.Seek(EncodeKey(key, txn.StartTS))
	if !itr.Valid() {
		return nil, nil
	}
	item := itr.Item()
	wts := decodeTimestamp(item.Key())
	if false == (txn.StartTS >= wts) {
		log.Fatalf("GetValue(%x) wts=%d;txn.ts=%d", key, wts, txn.StartTS)
	}
	wv, err := item.Value()
	if err != nil {
		log.Fatalf("GetValue(%x) value err:%s", key, err.Error())
	}
	write, err := ParseWrite(wv)
	if err != nil {
		log.Errorf("GetValue(%x) value(%x) err:%s", key, wv, err.Error())
		return nil, nil
	}
	if write.Kind != WriteKindPut {
		log.Debugf("GetValue(%x) was '%v'", key, write.Kind)
		return nil, nil
	}
	//2-find the value by write.StartTS;
	return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	// Your Code Here (4A).
	put := storage.Put{
		Key:   EncodeKey(key, txn.StartTS),
		Value: value,
		Cf:    engine_util.CfDefault,
	}
	log.TxnDbg("PutValue key=%x,value=%x", put.Key, put.Value)
	txn.writes = append(txn.writes, storage.Modify{put})
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	// Your Code Here (4A).
	del := storage.Delete{
		Key: EncodeKey(key, txn.StartTS),
		Cf:  engine_util.CfDefault,
	}
	log.TxnDbg("DeleteValue key=%x,", key)
	txn.writes = append(txn.writes, storage.Modify{del})
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// 找到与 txn.timestamp 相等的write.
//    即，从 txn.timestamp 往后遍历.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	log.Debugf("CurrentWrite(%x,%d)", key, txn.StartTS)
	// Your Code Here (4A).
	itr := txn.Reader.IterCF(engine_util.CfWrite)
	defer itr.Close()
	itr.Seek(key)
	lastKey := EncodeKey(key, txn.StartTS)
	lastKey = EncodeKey(key, 0)
	for itr.Valid() {
		item := itr.Item()
		switch bytes.Compare(item.Key(), lastKey) {
		case 1: //out of range;
			log.Errorf("%v out of range %v,%v(%d)", key, item.Key(), lastKey, txn.StartTS)
			return nil, 0, nil
		case 0, -1: //smaller,continue;
			write, wts, err := writeFromItem(item)
			if err != nil {
				log.Warnf("CurrentWrite err:%s", err.Error())
				continue
			} else if write != nil {
				if write.StartTS == txn.StartTS {
					return write, wts, err
				}
				if write.StartTS < txn.StartTS {
					//后面没有更小的了，可以退出了.
					//log.Warnf("no more writes %d", write.StartTS)
					return nil, 0, nil
				}
			}
			itr.Next()
			continue
		}
	}
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
// 查找最新的 Write.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	// Your Code Here (4A).
	itr := txn.Reader.IterCF(engine_util.CfWrite)
	defer itr.Close()
	itr.Seek(key)
	if !itr.Valid() {
		return nil, 0, nil
	}
	uk := DecodeUserKey(itr.Item().Key())
	if !bytes.Equal(uk, key) {
		return nil, 0, nil
	}
	return writeFromItem(itr.Item())
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// decodeTimestamp takes a key + timestamp and returns the timestamp part.
func decodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

func writeFromItem(item engine_util.DBItem) (*Write, uint64, error) {
	if item == nil {
		return nil, 0, nil
	}
	uVal, err := item.Value()
	if err != nil {
		return nil, 0, err
	}
	w, err := ParseWrite(uVal)
	if err != nil {
		return nil, 0, err
	}
	return w, decodeTimestamp(item.Key()), nil
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
