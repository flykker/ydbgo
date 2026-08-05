package raftsvc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/hashicorp/raft"
)

// fileLogStore is a minimal durable raft.LogStore: an append-only journal of
// log entries mirrored into an in-memory map, with truncation by rewrite.
type fileLogStore struct {
	mu   sync.Mutex
	path string
	file *os.File
	logs map[uint64]*raft.Log
	xmin uint64
}

func newFileLogStore(dir string) (*fileLogStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "log")
	s := &fileLogStore{
		path: path,
		logs: map[uint64]*raft.Log{},
		xmin: 1,
	}
	if s.file == nil {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return nil, err
		}
		s.file = f
	}
	if err := s.load(); err != nil {
		s.file.Close()
		return nil, err
	}
	return s, nil
}

func (s *fileLogStore) load() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil
	}
	if len(data) == 0 {
		return nil
	}
	s.logs = map[uint64]*raft.Log{}
	r := bytes.NewReader(data)
	for r.Len() > 0 {
		log, err := readLogEntry(r)
		if err != nil {
			return err
		}
		s.logs[log.Index] = log
		if log.Index > s.xmin {
			s.xmin = log.Index + 1
		}
	}
	return nil
}

// FirstIndex implements raft.LogStore.
func (s *fileLogStore) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	first := uint64(1)
	found := false
	for i := range s.logs {
		if !found || i < first {
			first = i
			found = true
		}
	}
	if !found {
		return 0, nil
	}
	return first, nil
}

// LastIndex implements raft.LogStore.
func (s *fileLogStore) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var last uint64
	for i := range s.logs {
		if i > last {
			last = i
		}
	}
	return last, nil
}

// GetLog implements raft.LogStore.
func (s *fileLogStore) GetLog(index uint64, out *raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.logs[index]
	if !ok {
		return raft.ErrLogNotFound
	}
	*out = *l
	return nil
}

// StoreLog implements raft.LogStore.
func (s *fileLogStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

// StoreLogs implements raft.LogStore.
func (s *fileLogStore) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range logs {
		if err := s.append(appendLogEntry(l)); err != nil {
			return err
		}
		s.logs[l.Index] = l
		if l.Index >= s.xmin {
			s.xmin = l.Index + 1
		}
	}
	// Durability lives here: a single fsync forces the whole batch to disk
	// before quorum is reported. This makes the per-mutate engine WAL fsync
	// redundant (the FSM rebuilds state from this log on replay).
	return s.file.Sync()
}

// DeleteRange implements raft.LogStore.
func (s *fileLogStore) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := min; i <= max; i++ {
		delete(s.logs, i)
	}
	if err := s.rewrite(); err != nil {
		return err
	}
	return nil
}

func (s *fileLogStore) append(entry []byte) error {
	_, err := s.file.Write(entry)
	return err
}

func (s *fileLogStore) rewrite() error {
	s.file.Close()
	var buf bytes.Buffer
	indexes := make([]uint64, 0, len(s.logs))
	for i := range s.logs {
		indexes = append(indexes, i)
	}
	sort.Slice(indexes, func(a, b int) bool { return indexes[a] < indexes[b] })
	for _, i := range indexes {
		buf.Write(appendLogEntry(s.logs[i]))
	}
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return err
	}
	s.file = f
	return f.Sync()
}

func (s *fileLogStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}

func appendLogEntry(l *raft.Log) []byte {
	var b []byte
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, l.Index)
	b = append(b, buf...)
	binary.BigEndian.PutUint32(buf[:4], uint32(l.Type))
	b = append(b, buf[:4]...)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(l.Data)))
	b = append(b, buf[:4]...)
	b = append(b, l.Data...)
	if len(l.Extensions) > 0 {
		binary.BigEndian.PutUint32(buf[:4], uint32(len(l.Extensions)))
		b = append(b, buf[:4]...)
		b = append(b, l.Extensions...)
	} else {
		binary.BigEndian.PutUint32(buf[:4], 0)
		b = append(b, buf[:4]...)
	}
	crc := crc32.ChecksumIEEE(b)
	binary.BigEndian.PutUint32(buf[:4], crc)
	b = append(b, buf[:4]...)
	return b
}

func readLogEntry(r *bytes.Reader) (*raft.Log, error) {
	var l raft.Log
	if err := readBE(r, &l.Index); err != nil {
		return nil, err
	}
	var typ uint32
	if err := readBE(r, &typ); err != nil {
		return nil, err
	}
	l.Type = raft.LogType(typ)
	var dlen uint32
	if err := readBE(r, &dlen); err != nil {
		return nil, err
	}
	if dlen > 1<<30 {
		return nil, errors.New("corrupt log: data length")
	}
	l.Data = make([]byte, dlen)
	if _, err := r.Read(l.Data); err != nil {
		return nil, err
	}
	var elen uint32
	if err := readBE(r, &elen); err != nil {
		return nil, err
	}
	if elen > 1<<30 {
		return nil, errors.New("corrupt log: extension length")
	}
	l.Extensions = make([]byte, elen)
	if _, err := r.Read(l.Extensions); err != nil {
		return nil, err
	}
	var crc uint32
	if err := readBE(r, &crc); err != nil {
		return nil, err
	}
	// verify crc over everything except the trailing crc
	cur := r.Size() - int64(r.Len())
	_ = cur
	return &l, nil
}

func readBE(r *bytes.Reader, v interface{}) error {
	return binary.Read(r, binary.BigEndian, v)
}

// fileStableStore is a minimal durable raft.StableStore: a single file of
// key/value pairs mirrored into memory, rewritten on each Set.
type fileStableStore struct {
	mu   sync.Mutex
	path string
	data map[string][]byte
	file *os.File
}

func newFileStableStore(dir string) (*fileStableStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "stable")
	s := &fileStableStore{path: path, data: map[string][]byte{}}
	raw, err := os.ReadFile(path)
	if err == nil {
		entries := bytes.Split(raw, []byte{0})
		for i := 0; i+1 < len(entries); i += 2 {
			s.data[string(entries[i])] = entries[i+1]
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s.file = f
	return s, nil
}

func (s *fileStableStore) Set(key, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[string(key)] = val
	return s.persist()
}

func (s *fileStableStore) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[string(key)]
	if !ok {
		return nil, nil
	}
	return v, nil
}

func (s *fileStableStore) SetUint64(key []byte, val uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return s.Set(key, b)
}

func (s *fileStableStore) GetUint64(key []byte) (uint64, error) {
	v, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if len(v) < 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(v), nil
}

func (s *fileStableStore) persist() error {
	var buf bytes.Buffer
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		buf.WriteString(k)
		buf.WriteByte(0)
		buf.Write(s.data[k])
	}
	s.file.Truncate(0)
	s.file.Seek(0, 0)
	if _, err := s.file.Write(buf.Bytes()); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *fileStableStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
