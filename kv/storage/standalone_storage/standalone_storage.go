package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	conf    *config.Config
	engines *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	os.MkdirAll(conf.DBPath, os.ModePerm)
	kvPath := filepath.Join(conf.DBPath, "kv")
	kvDB := engine_util.CreateDB(kvPath, false)

	return &StandAloneStorage{
		conf:    conf,
		engines: engine_util.NewEngines(kvDB, nil, kvPath, ""),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	if s.engines.Kv == nil {
		return errors.New("db is nil")
	}
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.engines.Kv != nil {
		err := s.engines.Kv.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneStorageReader(s), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return s.engines.Kv.Update(func(txn *badger.Txn) error {
		for _, m := range batch {
			switch m.Data.(type) {
			case storage.Put:
				put := m.Data.(storage.Put)
				err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
				if err != nil {
					return err
				}
			case storage.Delete:
				del := m.Data.(storage.Delete)
				err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}
