package standalone_storage

import (
	"fmt"
	"strings"

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
	engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	if conf == nil {
		return nil
	}
	storage := &StandAloneStorage{}
	standalonePath := conf.DBPath + "/standalone"
	raftPath := conf.DBPath + "/raft"
	standalone := engine_util.CreateDB(standalonePath, false)
	raft := engine_util.CreateDB(raftPath, true)
	storage.Engines = *engine_util.NewEngines(standalone, raft, standalonePath, raftPath)
	return storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.Kv.Close(); err != nil {
		return err
	}
	if err := s.Raft.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := &storageReader{
		Db: s.Kv,
	}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	bt := &engine_util.WriteBatch{}
	for i := range batch {
		var cf string
		if cf = batch[i].Cf(); len(cf) == 0 {
			return fmt.Errorf("empty cf")
		}
		var key []byte
		if key = batch[i].Key(); key == nil {
			return fmt.Errorf("bad key")
		}
		var value []byte
		if value = batch[i].Value(); value != nil {
			bt.SetCF(cf, key, value)
		} else {
			bt.DeleteCF(cf, key)
		}
	}
	return bt.WriteToDB(s.Kv)
}

type storageReader struct {
	Db *badger.DB
	// TODO:store used txns to discard and close in the end together
}

// Close implements storage.StorageReader.
func (s *storageReader) Close() {
}

// GetCF implements storage.StorageReader.
func (s *storageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCF(s.Db, cf, key)
	if err != nil && strings.Contains(err.Error(), "not found") {
		return nil, nil
	}
	return val, err
}

// IterCF implements storage.StorageReader.
func (s *storageReader) IterCF(cf string) engine_util.DBIterator {
	txn := s.Db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

var _ storage.StorageReader = &storageReader{}
