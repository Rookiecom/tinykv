package standalone_storage

import (
	"errors"
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (s StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := s.txn.Get(engine_util.KeyWithCF(cf, key))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var val []byte
	val, err = item.Value()
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (s StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s StandAloneStorageReader) Close() {
	s.txn.Discard()
}

func NewStandAloneStorageReader(s *StandAloneStorage) StandAloneStorageReader {
	return StandAloneStorageReader{txn: s.engines.Kv.NewTransaction(false)}
}
