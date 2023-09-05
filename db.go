package luffy

import (
	"os"
	"sync"
)

type DB struct {
	rwlock sync.RWMutex
	*dal
}

func Open(path string, options *Options) (*DB, error) {
	options.pageSize = os.Getpagesize()

	dal, err := newDal(path, options)
	if err != nil {
		return nil, err
	}

	db := &DB{
		sync.RWMutex{},
		dal,
	}
	return db, nil
}

func (db *DB) Close() error {
	return db.close()
}

// for opening transactions and acquirin locks

func (db *DB) ReadTx() *tx {
	db.rwlock.RLock()
	return newTx(db, false)
}

func (db *DB) WriteTx() *tx {
	db.rwlock.Lock()
	return newTx(db, true)
}
