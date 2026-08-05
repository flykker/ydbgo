package storage

import (
	"errors"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketMeta   = []byte("meta")   // schema definitions
	bucketTables = []byte("tables") // parent of per-table row buckets
)

// bboltStore is the bbolt-backed store. Row buckets are nested under
// "tables"; schema definitions live under "meta", keyed by table name.
type bboltStore struct {
	db *bolt.DB
}

func openBbolt(path string) (*bboltStore, error) {
	db, err := bolt.Open(path, 0o644, nil)
	if err != nil {
		return nil, err
	}
	// ensure top-level buckets exist
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketMeta); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketTables); err != nil {
			return err
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &bboltStore{db: db}, nil
}

func (s *bboltStore) Close() error { return s.db.Close() }

func (s *bboltStore) begin() (storeTx, error) {
	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &bboltTx{tx: tx}, nil
}

func (s *bboltStore) view(fn func(tx storeTx) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		return fn(&bboltTx{tx: tx})
	})
}

// bboltTx wraps a *bolt.Tx as a storeTx.
type bboltTx struct {
	tx *bolt.Tx
}

func (t *bboltTx) commit() error {
	if t.tx == nil {
		return errors.New("bbolt: tx already finished")
	}
	if t.tx.DB() == nil {
		return errors.New("bbolt: db closed")
	}
	err := t.tx.Commit()
	t.tx = nil
	return err
}

func (t *bboltTx) rollback() error {
	if t.tx == nil {
		return nil
	}
	err := t.tx.Rollback()
	t.tx = nil
	return err
}

func (t *bboltTx) meta() (*bolt.Bucket, error) {
	b := t.tx.Bucket(bucketMeta)
	if b == nil {
		return nil, errors.New("bbolt: meta bucket missing")
	}
	return b, nil
}

// tableRows returns the row bucket for a table, creating it if the
// transaction is writable and the bucket does not exist yet.
func (t *bboltTx) tableRows(name string) (*bolt.Bucket, error) {
	parent := t.tx.Bucket(bucketTables)
	if parent == nil {
		return nil, errors.New("bbolt: tables bucket missing")
	}
	if b := parent.Bucket([]byte(name)); b != nil {
		return b, nil
	}
	if t.tx.Writable() {
		return parent.CreateBucketIfNotExists([]byte(name))
	}
	// missing bucket in a read-only view => empty table
	return nil, nil
}

func (t *bboltTx) schemaPut(name string, def []byte) error {
	mb, err := t.meta()
	if err != nil {
		return err
	}
	return mb.Put([]byte(name), def)
}

func (t *bboltTx) schemaGet(name string) ([]byte, error) {
	mb, err := t.meta()
	if err != nil {
		return nil, err
	}
	return mb.Get([]byte(name)), nil
}

func (t *bboltTx) schemaDelete(name string) error {
	mb, err := t.meta()
	if err != nil {
		return err
	}
	return mb.Delete([]byte(name))
}

func (t *bboltTx) schemaNames() ([]string, error) {
	names := []string{}
	mb, err := t.meta()
	if err != nil {
		return nil, err
	}
	c := mb.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		names = append(names, string(k))
	}
	return names, nil
}

func (t *bboltTx) rowPut(table string, key []byte, val []byte) error {
	b, err := t.tableRows(table)
	if err != nil {
		return err
	}
	if b == nil {
		return errors.New("bbolt: bucket missing for " + table)
	}
	return b.Put(key, val)
}

func (t *bboltTx) rowGet(table string, key []byte) ([]byte, error) {
	b, err := t.tableRows(table)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	return b.Get(key), nil
}

func (t *bboltTx) rowDelete(table string, key []byte) error {
	b, err := t.tableRows(table)
	if err != nil {
		return err
	}
	if b == nil {
		return nil
	}
	return b.Delete(key)
}

func (t *bboltTx) rowEach(table string, fn func(k, v []byte) error) error {
	b, err := t.tableRows(table)
	if err != nil {
		return err
	}
	if b == nil {
		return nil
	}
	return b.ForEach(fn)
}

func (t *bboltTx) dropTable(name string) error {
	if err := t.schemaDelete(name); err != nil {
		return err
	}
	parent := t.tx.Bucket(bucketTables)
	if parent == nil {
		return nil
	}
	if parent.Bucket([]byte(name)) != nil {
		return parent.DeleteBucket([]byte(name))
	}
	return nil
}
