package storage

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"sort"

	"ydbgo/internal/kv"
)

// CSTORE is a columnar (OLAP) backend backed by internal/kv (MVCC). Row data
// is stored column-major so a projection over one column reads one contiguous
// range; this is the storage shape OLAP scans/aggregates want. Layout inside
// one kv.Store key space:
//
//	's' \x00 <table>                       schema definition
//	'r' \x00 <table> \x00 <pk>             row marker -> column count
//	'c' \x00 <table> \x00 <colIdx:4BE> \x00 <pk>   one cell of one column
//
// A row is an encoded (count + variants) blob split across its column cells.
// Reads reconstruct rows from their cells; column-granular scans can skip the
// cells of untouched columns entirely.
const (
	cstoreSchemaTag byte = 's'
	cstoreRowTag    byte = 'r'
	cstoreColTag    byte = 'c'
)

func cstoreColKey(table string, colIdx int, pk []byte) []byte {
	var idx [4]byte
	binary.BigEndian.PutUint32(idx[:], uint32(colIdx))
	b := make([]byte, 0, 4+len(table)+4+len(pk))
	b = append(b, cstoreColTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	b = append(b, idx[:]...)
	b = append(b, 0x00)
	return append(b, pk...)
}

func cstoreColPrefix(table string, colIdx int) []byte {
	var idx [4]byte
	binary.BigEndian.PutUint32(idx[:], uint32(colIdx))
	b := make([]byte, 0, 4+len(table)+4)
	b = append(b, cstoreColTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	b = append(b, idx[:]...)
	b = append(b, 0x00)
	return b
}

func cstoreColBounds(table string, colIdx int) (lower, upper []byte) {
	lower = cstoreColPrefix(table, colIdx)
	upper = append([]byte(nil), lower...)
	upper[len(upper)-1]++
	return lower, upper
}

// cstoreColAllBounds covers every column cell of a table (all colIdx).
func cstoreColAllBounds(table string) (lower, upper []byte) {
	lower = append([]byte{cstoreColTag, 0x00}, table...)
	lower = append(lower, 0x00)
	upper = append([]byte(nil), lower...)
	upper[len(upper)-1]++
	return lower, upper
}

func cstoreRowKey(table string, pk []byte) []byte {
	b := make([]byte, 0, 3+len(table)+len(pk))
	b = append(b, cstoreRowTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	return append(b, pk...)
}

func cstoreRowPrefix(table string) []byte {
	b := make([]byte, 0, 3+len(table))
	b = append(b, cstoreRowTag, 0x00)
	b = append(b, table...)
	b = append(b, 0x00)
	return b
}

func cstoreRowBounds(table string) (lower, upper []byte) {
	lower = cstoreRowPrefix(table)
	upper = append([]byte(nil), lower...)
	upper[len(upper)-1]++
	return lower, upper
}

// cstoreStore is the ENGINE=CSTORE store binding.
type cstoreStore struct {
	st *kv.Store
}

func openCStore(dir string) (*cstoreStore, error) {
	s, err := kv.Open(filepath.Join(dir, "engine_cstore"))
	if err != nil {
		return nil, err
	}
	return &cstoreStore{st: s}, nil
}

func (s *cstoreStore) Close() error { return s.st.Close() }

func (s *cstoreStore) begin() (storeTx, error) {
	return &cstoreTx{s: s, overlay: map[string]*pending{}}, nil
}

func (s *cstoreStore) view(fn func(tx storeTx) error) error {
	return fn(&cstoreTx{s: s})
}

// cstoreTx buffers writes and flushes as one kv.Apply on commit, mirroring
// kvTx's overlay/read-your-writes semantics with the columnar key layout.
type cstoreTx struct {
	s       *cstoreStore
	overlay map[string]*pending
	ops     []kv.Op
}

func (t *cstoreTx) pkey(key []byte) string { return string(key) }

func (t *cstoreTx) put(key, value []byte) {
	k := t.pkey(key)
	t.overlay[k] = &pending{value: append([]byte(nil), value...)}
	t.ops = append(t.ops, kv.Op{Key: append([]byte(nil), key...), Value: value})
}

func (t *cstoreTx) del(key []byte) {
	k := t.pkey(key)
	t.overlay[k] = &pending{del: true}
	t.ops = append(t.ops, kv.Op{Key: append([]byte(nil), key...), Delete: true})
}

func (t *cstoreTx) get(key []byte) ([]byte, error) {
	if p, ok := t.overlay[t.pkey(key)]; ok {
		if p.del {
			return nil, nil
		}
		return p.value, nil
	}
	v, ok, err := t.s.st.Get(0, key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return v, nil
}

func (t *cstoreTx) each(lower, upper []byte, fn func(k, v []byte) error) error {
	// Fast path: no buffered writes, so the store's byte order is already the
	// result order and there is nothing to overlay/merge.
	if len(t.ops) == 0 {
		return t.s.st.Range(0, lower, upper, func(key, val []byte, deleted bool) error {
			if deleted {
				return nil
			}
			return fn(key, val)
		})
	}
	ordered := []string{}
	vals := map[string][]byte{}
	t.s.st.Range(0, lower, upper, func(key, val []byte, deleted bool) error {
		if !deleted {
			ordered = append(ordered, t.pkey(key))
			vals[t.pkey(key)] = append([]byte(nil), val...)
		}
		return nil
	})
	for _, op := range t.ops {
		k := t.pkey(op.Key)
		if bytes.Compare(op.Key, lower) < 0 || bytes.Compare(op.Key, upper) >= 0 {
			continue
		}
		if op.Delete {
			if _, was := vals[k]; was {
				delete(vals, k)
			}
			continue
		}
		if _, was := vals[k]; !was {
			ordered = append(ordered, k)
		}
		vals[k] = op.Value
	}
	sort.Strings(ordered)
	for _, k := range ordered {
		if err := fn([]byte(k), vals[k]); err != nil {
			return err
		}
	}
	return nil
}

func (t *cstoreTx) schemaPut(name string, def []byte) error {
	t.put(kvSchemaKey(name), def)
	return nil
}
func (t *cstoreTx) schemaGet(name string) ([]byte, error) { return t.get(kvSchemaKey(name)) }
func (t *cstoreTx) schemaDelete(name string) error        { t.del(kvSchemaKey(name)); return nil }
func (t *cstoreTx) schemaNames() ([]string, error) {
	names := []string{}
	lower, upper := kvSchemaBounds()
	err := t.each(lower, upper, func(k, _ []byte) error {
		names = append(names, string(k[len(lower):]))
		return nil
	})
	return names, err
}

// splitRow decodes an encoded row (count + variants) into its per-column cell
// blobs (each a variant in the codec's format).
func splitRow(encoded []byte) (int, [][]byte, error) {
	r := makeReader(encoded)
	n := int(r.Var())
	cells := make([][]byte, n)
	// re-encode each variant into its own blob using the builder
	for i := 0; i < n; i++ {
		v := r.Variant()
		if r.err != nil {
			return 0, nil, r.err
		}
		b := makeBuilder()
		b.Variant(v)
		cells[i] = b.Bytes()
	}
	return n, cells, nil
}

// joinRow reassembles the encoded row blob from its column cells.
func joinRow(cells [][]byte) []byte {
	b := makeBuilder()
	b.Var(int64(len(cells)))
	for _, c := range cells {
		b.buf = append(b.buf, c...)
	}
	return b.Bytes()
}

func (t *cstoreTx) rowPut(table string, key []byte, val []byte) error {
	n, cells, err := splitRow(val)
	if err != nil {
		return err
	}
	var cnt [8]byte
	binary.BigEndian.PutUint64(cnt[:], uint64(n))
	t.put(cstoreRowKey(table, key), cnt[:])
	for i, c := range cells {
		t.put(cstoreColKey(table, i, key), c)
	}
	return nil
}

func (t *cstoreTx) rowGet(table string, key []byte) ([]byte, error) {
	cntRaw, err := t.get(cstoreRowKey(table, key))
	if err != nil || len(cntRaw) == 0 {
		return nil, err
	}
	n := int(binary.BigEndian.Uint64(cntRaw))
	cells := make([][]byte, n)
	for i := 0; i < n; i++ {
		c, err := t.get(cstoreColKey(table, i, key))
		if err != nil {
			return nil, err
		}
		if len(c) == 0 {
			return nil, nil // partially deleted row
		}
		cells[i] = c
	}
	return joinRow(cells), nil
}

func (t *cstoreTx) rowDelete(table string, key []byte) error {
	cntRaw, err := t.get(cstoreRowKey(table, key))
	if err != nil {
		return err
	}
	t.del(cstoreRowKey(table, key))
	if len(cntRaw) == 0 {
		return nil
	}
	n := int(binary.BigEndian.Uint64(cntRaw))
	for i := 0; i < n; i++ {
		t.del(cstoreColKey(table, i, key))
	}
	return nil
}

func (t *cstoreTx) rowEach(table string, fn func(k, v []byte) error) error {
	lower, upper := cstoreRowBounds(table)
	pl := len(lower)
	return t.each(lower, upper, func(k, _ []byte) error {
		encoded, err := t.rowGet(table, k[pl:])
		if err != nil || encoded == nil {
			return err
		}
		return fn(k[pl:], encoded)
	})
}

// colRowKeys iterates the primary keys of a table in sorted order, reading
// only row markers (no cell reconstruction).
func (t *cstoreTx) colRowKeys(table string, fn func(pk []byte) error) error {
	return t.colRowKeysRange(table, nil, nil, fn)
}

// colRowKeysRange is colRowKeys restricted to pks within [plLower, plUpper)
// (encoded pk byte bounds; nil = unbounded on that side).
func (t *cstoreTx) colRowKeysRange(table string, plLower, plUpper []byte, fn func(pk []byte) error) error {
	if len(plLower) > 0 && len(plUpper) > 0 && bytes.Compare(plLower, plUpper) >= 0 {
		return nil // empty window: an inverted range (e.g. pk >= hi AND pk < lo) matches nothing
	}
	prefix := cstoreRowPrefix(table)
	lower := prefix
	if len(plLower) > 0 {
		lower = append(append([]byte(nil), prefix...), plLower...)
	}
	upper := append([]byte(nil), prefix...)
	upper[len(upper)-1]++
	if len(plUpper) > 0 {
		upper = append(prefix, plUpper...)
	}
	pl := len(prefix)
	return t.each(lower, upper, func(k, _ []byte) error {
		return fn(k[pl:])
	})
}

// colEach iterates the cells of one column, delivering each pk and its cell
// blob (a single variant).
func (t *cstoreTx) colEach(table string, colIdx int, fn func(pk, cell []byte) error) error {
	return t.colEachRange(table, colIdx, nil, nil, fn)
}

// colEachRange is colEach restricted to pks within [plLower, plUpper) (encoded
// pk byte bounds; nil = unbounded on that side).
func (t *cstoreTx) colEachRange(table string, colIdx int, plLower, plUpper []byte, fn func(pk, cell []byte) error) error {
	if len(plLower) > 0 && len(plUpper) > 0 && bytes.Compare(plLower, plUpper) >= 0 {
		return nil // empty window: an inverted range matches nothing
	}
	prefix := cstoreColPrefix(table, colIdx)
	lower := prefix
	if len(plLower) > 0 {
		lower = append(append([]byte(nil), prefix...), plLower...)
	}
	upper := append([]byte(nil), prefix...)
	upper[len(upper)-1]++
	if len(plUpper) > 0 {
		upper = append(prefix, plUpper...)
	}
	pl := len(prefix)
	return t.each(lower, upper, func(k, v []byte) error {
		return fn(k[pl:], v)
	})
}

// rowDeleteRange removes every row whose pk lies within [plLower, plUpper)
// (encoded pk byte bounds; nil = unbounded). Pks are collected first so a
// delete in the same batch cannot disturb the iteration.
func (t *cstoreTx) rowDeleteRange(table string, plLower, plUpper []byte, affected *int64) error {
	var pks [][]byte
	if err := t.colRowKeysRange(table, plLower, plUpper, func(pk []byte) error {
		pks = append(pks, append([]byte(nil), pk...))
		return nil
	}); err != nil {
		return err
	}
	for _, pk := range pks {
		if err := t.rowDelete(table, pk); err != nil {
			return err
		}
	}
	*affected = int64(len(pks))
	return nil
}

func (t *cstoreTx) rowDeleteAll(table string) error {
	lower, upper := cstoreRowBounds(table)
	var rowKeys [][]byte
	t.each(lower, upper, func(k, _ []byte) error {
		rowKeys = append(rowKeys, append([]byte(nil), k...))
		return nil
	})
	for _, k := range rowKeys {
		if err := t.rowDelete(table, k[len(lower):]); err != nil {
			return err
		}
	}
	// drop any orphan column cells (no marker, e.g. after partial writes)
	cl, cu := cstoreColAllBounds(table)
	var colKeys [][]byte
	t.each(cl, cu, func(k, _ []byte) error {
		colKeys = append(colKeys, append([]byte(nil), k...))
		return nil
	})
	for _, k := range colKeys {
		t.del(k)
	}
	return nil
}

func (t *cstoreTx) dropTable(name string) error {
	if err := t.schemaDelete(name); err != nil {
		return err
	}
	return t.rowDeleteAll(name)
}

func (t *cstoreTx) commit() error {
	if len(t.ops) == 0 {
		return nil
	}
	if err := t.s.st.Apply(0, t.ops); err != nil {
		return err
	}
	t.ops = nil
	t.overlay = map[string]*pending{}
	return nil
}

func (t *cstoreTx) rollback() error {
	t.ops = nil
	t.overlay = map[string]*pending{}
	return nil
}
