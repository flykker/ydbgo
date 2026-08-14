package kv

import "testing"

func TestDBGRangeDesc(t *testing.T) {
	s := newTestStore(t)
	s.Apply(1, []Op{{Key: []byte("a"), Value: []byte("a1")}, {Key: []byte("m"), Value: []byte("m1")}})
	s.Apply(2, []Op{{Key: []byte("a"), Value: []byte("a2")}, {Key: []byte("z"), Value: []byte("z1")}})
	s.Apply(3, []Op{{Key: []byte("z"), Delete: true}})
	var keys []string
	err := s.RangeDesc(0, nil, nil, func(k, v []byte, del bool) error {
		keys = append(keys, string(k))
		t.Logf("emit key=%s del=%v", k, del)
		return nil
	})
	t.Logf("err=%v keys=%v", err, keys)
}