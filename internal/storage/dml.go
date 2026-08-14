package storage

import (
	"sort"
)

type mutatePut struct {
	key    string
	values map[string]sqlValue
}

// encodeMutate encodes a row mutation record.
func (e *Engine) encodeMutate(table string, p mutatePut) []byte {
	b := makeBuilder()
	b.Byte(recMutate)
	b.Str(table)
	b.Byte(0) // op: put
	b.Str(p.key)
	b.Var(int64(len(p.values)))
	keys := make([]string, 0, len(p.values))
	for k := range p.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		b.Str(k)
		b.Variant(p.values[k])
	}
	return b.Bytes()
}

func (e *Engine) unmarshalMutate(buf []byte) (table string, put mutatePut, isPut bool, err error) {
	r := makeReader(buf)
	r.Byte() // recMutate
	table = r.Str()
	op := r.Byte()
	isPut = op == 0
	key := r.Str()
	n := int(r.Var())
	vals := map[string]sqlValue{}
	for i := 0; i < n; i++ {
		k := r.Str()
		vals[k] = r.Variant()
	}
	if r.err != nil {
		return table, mutatePut{}, false, r.err
	}
	return table, mutatePut{key: key, values: vals}, isPut, nil
}

// encodeKVData serializes a raw byte-KV entry record (snapshot replay/apply).
func (e *Engine) encodeKVData(table string, key, value []byte) []byte {
	b := makeBuilder()
	b.Byte(recKVData)
	b.Str(table)
	b.Str(string(key))
	b.Str(string(value))
	return b.Bytes()
}

func (e *Engine) unmarshalKVData(buf []byte) (table, key, value string, err error) {
	r := makeReader(buf)
	r.Byte() // recKVData
	table = r.Str()
	key = r.Str()
	value = r.Str()
	return table, key, value, r.err
}
