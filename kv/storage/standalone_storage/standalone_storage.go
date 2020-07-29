package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	obj := engine_util.CreateDB("alone", conf)
	return &StandAloneStorage{obj}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return &StandAloneReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if len(batch) > 0 {
		err := s.db.Update(func(txn *badger.Txn) error {
			for _, entry := range batch {
				var err1 error
				storeKey := engine_util.KeyWithCF(entry.Cf(), entry.Key())
				if len(entry.Value()) == 0 {
					err1 = txn.Delete(storeKey)
				} else {
					err1 = txn.SetEntry(&badger.Entry{
						Key:   storeKey,
						Value: entry.Value(),
					})
				}
				if err1 != nil {
					return err1
				}
			}
			return nil
		})
		return err
	}
	return nil
}

type StandAloneReader struct {
	txn *badger.Txn
}

var emptyValue []byte

func (sar *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := sar.txn.Get(engine_util.KeyWithCF(cf, key))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	if item.IsEmpty() {
		return nil, nil
	}
	v, err := item.Value()
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return v, err
}
func (sar *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sar.txn)
}
func (sar *StandAloneReader) Close() {
	sar.txn.Discard()
}
