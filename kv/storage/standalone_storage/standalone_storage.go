package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf *config.Config
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	storage := StandAloneStorage{
		conf: conf,
		db: nil,
	}

	return &storage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	opt := badger.DefaultOptions
	opt.Dir = s.conf.StoreAddr
	opt.ValueDir = s.conf.StoreAddr
	db, err := badger.Open(opt)
	if err != nil {
		log.Fatal("badger open error")
		return err
	}

	s.db = db

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	if err != nil {
		 log.Fatal(err)
		 return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	reader := NewStandaloneStorageReader(s)

	return &reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, mod := range batch {
		switch mod.Data.(type) {
		case storage.Put:
			err := engine_util.PutCF(s.db, mod.Cf(), mod.Key(), mod.Value())
			if err != nil {
				log.Fatal(err)
				return err
			}
		case storage.Delete:
			err := engine_util.DeleteCF(s.db, mod.Cf(), mod.Key())
			if err != nil {
				log.Fatal(err)
				return err
			}

		}
	}

	return nil
}

func NewStandaloneStorageReader(s *StandAloneStorage)  StandaloneStorageReader {
	return StandaloneStorageReader{
		txn: s.db.NewTransaction(false),
	}
}


type StandaloneStorageReader struct {
	txn *badger.Txn
}

func (reader *StandaloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err != nil {
		 return nil, err
	}

	return value, err
}

func  (reader *StandaloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func  (reader *StandaloneStorageReader) Close() {
	// 关闭事务
	reader.txn.Discard()
}

