package kv

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "kv"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestPutGetRevisions(t *testing.T) {
	s := newTestStore(t)
	if err := s.Apply(1, []Op{{Key: []byte("a"), Value: []byte("v1")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(2, []Op{{Key: []byte("a"), Value: []byte("v2")}}); err != nil {
		t.Fatal(err)
	}
	// read latest
	v, ok, err := s.Get(0, []byte("a"))
	if err != nil || !ok || string(v) != "v2" {
		t.Fatalf("latest get: v=%q ok=%v err=%v", v, ok, err)
	}
	// read past revision
	v, ok, err = s.Get(1, []byte("a"))
	if err != nil || !ok || string(v) != "v1" {
		t.Fatalf("rev1 get: v=%q ok=%v err=%v", v, ok, err)
	}
	if got := s.Latest(); got != 2 {
		t.Fatalf("latest rev=%d want 2", got)
	}
	// missing key
	if _, ok, _ := s.Get(0, []byte("zz")); ok {
		t.Fatal("expected missing key")
	}
}

func TestDeleteTombstone(t *testing.T) {
	s := newTestStore(t)
	s.Apply(1, []Op{{Key: []byte("k"), Value: []byte("x")}})
	s.Apply(2, []Op{{Key: []byte("k"), Delete: true}})
	if _, ok, _ := s.Get(0, []byte("k")); ok {
		t.Fatal("key should be gone after tombstone")
	}
	// but visible at the earlier revision
	v, ok, _ := s.Get(1, []byte("k"))
	if !ok || string(v) != "x" {
		t.Fatalf("rev1=%q ok=%v", v, ok)
	}
}

func TestRevisionMonotonic(t *testing.T) {
	s := newTestStore(t)
	if err := s.Apply(5, []Op{{Key: []byte("k"), Value: []byte("x")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(3, []Op{{Key: []byte("k"), Value: []byte("y")}}); err == nil {
		t.Fatal("expected revision regression error")
	}
	// Apply at same rev is also rejected
	if err := s.Apply(5, []Op{{Key: []byte("k"), Value: []byte("z")}}); err == nil {
		t.Fatal("expected duplicate-revision error")
	}
}

func TestRangeAndPrefix(t *testing.T) {
	s := newTestStore(t)
	keys := []string{"aa", "ab", "b", "ba"}
	for i, k := range keys {
		s.Apply(Revision(i+1), []Op{{Key: []byte(k), Value: []byte(k)}})
	}
	// range [ab, b) → ab only
	var got []string
	s.Range(0, []byte("ab"), []byte("b"), func(k, v []byte, del bool) error {
		got = append(got, string(k))
		return nil
	})
	if len(got) != 1 || got[0] != "ab" {
		t.Fatalf("range [ab,b): %v", got)
	}
	// prefix "b" → b, ba
	got = nil
	s.Prefix(0, []byte("b"), func(k, v []byte, del bool) error {
		got = append(got, string(k))
		return nil
	})
	if len(got) != 2 || got[0] != "b" || got[1] != "ba" {
		t.Fatalf("prefix b: %v", got)
	}
	// prefix a at latest revision shows all a*
	got = nil
	s.Prefix(0, []byte("a"), func(k, v []byte, del bool) error {
		got = append(got, string(k))
		return nil
	})
	if len(got) != 2 {
		t.Fatalf("prefix a: %v", got)
	}
}

func TestRangeDesc(t *testing.T) {
	s := newTestStore(t)
	// versions: x has v1 then v2 (newest wins), z deleted, m single
	s.Apply(1, []Op{{Key: []byte("a"), Value: []byte("a1")}, {Key: []byte("m"), Value: []byte("m1")}})
	s.Apply(2, []Op{{Key: []byte("a"), Value: []byte("a2")}, {Key: []byte("z"), Value: []byte("z1")}})
	s.Apply(3, []Op{{Key: []byte("z"), Delete: true}})
	// reverse full range: z(deleted), m, a(2); z is reported as a tombstone
	var keys, vals []string
	var dels []bool
	s.RangeDesc(0, nil, nil, func(k, v []byte, del bool) error {
		keys = append(keys, string(k))
		vals = append(vals, string(v))
		dels = append(dels, del)
		return nil
	})
	want := []string{"z", "m", "a"} // reverse byte order
	if len(keys) != len(want) {
		t.Fatalf("RangeDesc keys: %v", keys)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("RangeDesc order: %v want %v", keys, want)
		}
	}
	if !dels[0] || vals[1] != "m1" || dels[1] || vals[2] != "a2" || dels[2] {
		t.Fatalf("RangeDesc vals/dels: keys=%v vals=%v dels=%v", keys, vals, dels)
	}
	// reverse over [a, z): excludes z, includes m and a
	keys = nil
	s.RangeDesc(0, []byte("a"), []byte("z"), func(k, v []byte, del bool) error {
		keys = append(keys, string(k))
		return nil
	})
	if len(keys) != 2 || keys[0] != "m" || keys[1] != "a" {
		t.Fatalf("RangeDesc [a,z): %v", keys)
	}
	// newest-version-wins at a past revision: at rev 1, a=1 (v2 not visible)
	var gotA string
	s.RangeDesc(1, nil, nil, func(k, v []byte, del bool) error {
		if string(k) == "a" && !del {
			gotA = string(v)
		}
		return nil
	})
	if gotA != "a1" {
		t.Fatalf("RangeDesc at rev1 a=%q", gotA)
	}
	// inverted range yields nothing
	keys = nil
	s.RangeDesc(0, []byte("z"), []byte("a"), func(k, v []byte, del bool) error {
		keys = append(keys, string(k))
		return nil
	})
	if len(keys) != 0 {
		t.Fatalf("inverted RangeDesc: %v", keys)
	}
}

func TestRangeDedupsNewest(t *testing.T) {
	s := newTestStore(t)
	s.Apply(1, []Op{{Key: []byte("x"), Value: []byte("1")}})
	s.Apply(2, []Op{{Key: []byte("x"), Value: []byte("2")}})
	s.Apply(3, []Op{{Key: []byte("y"), Value: []byte("3")}})
	var got map[string]string
	got = map[string]string{}
	s.Range(0, nil, nil, func(k, v []byte, del bool) error {
		got[string(k)] = string(v)
		return nil
	})
	if got["x"] != "2" || got["y"] != "3" {
		t.Fatalf("range: %v", got)
	}
}

func TestRangeAtRevision(t *testing.T) {
	s := newTestStore(t)
	s.Apply(1, []Op{{Key: []byte("a"), Value: []byte("1")}})
	s.Apply(2, []Op{{Key: []byte("a"), Value: []byte("2")}, {Key: []byte("b"), Value: []byte("b")}})
	s.Apply(3, []Op{{Key: []byte("a"), Delete: true}})
	// at rev 2 both keys visible with a=2
	var got map[string]string
	got = map[string]string{}
	s.Range(2, nil, nil, func(k, v []byte, del bool) error {
		if !del {
			got[string(k)] = string(v)
		}
		return nil
	})
	if got["a"] != "2" || got["b"] != "b" {
		t.Fatalf("at rev2: %v", got)
	}
	// at latest, a deleted → only b
	got = map[string]string{}
	s.Range(0, nil, nil, func(k, v []byte, del bool) error {
		if !del {
			got[string(k)] = string(v)
		}
		return nil
	})
	if len(got) != 1 || got["b"] != "b" {
		t.Fatalf("at latest: %v", got)
	}
}

func TestCompareAndSwap(t *testing.T) {
	s := newTestStore(t)
	ok, err := s.CompareAndSwap(1, []byte("k"), []byte("x"), true, Op{Key: []byte("k"), Value: []byte("x")})
	if err != nil || !ok {
		t.Fatalf("cas create: ok=%v err=%v", ok, err)
	}
	// wrong expected → fail
	ok, err = s.CompareAndSwap(2, []byte("k"), []byte("WRONG"), false, Op{Key: []byte("k"), Value: []byte("y")})
	if err != nil || ok {
		t.Fatalf("cas wrong expected: ok=%v err=%v", ok, err)
	}
	// correct expected → succeeds
	ok, err = s.CompareAndSwap(2, []byte("k"), []byte("x"), false, Op{Key: []byte("k"), Value: []byte("y")})
	if err != nil || !ok {
		t.Fatalf("cas update: ok=%v err=%v", ok, err)
	}
	v, _, _ := s.Get(0, []byte("k"))
	if string(v) != "y" {
		t.Fatalf("after cas: %q", v)
	}
	// expectMissing on existing key → fail
	ok, _ = s.CompareAndSwap(3, []byte("k"), nil, true, Op{Key: []byte("k"), Value: []byte("z")})
	if ok {
		t.Fatal("cas expectMissing should fail on existing key")
	}
}

func TestWatch(t *testing.T) {
	s := newTestStore(t)
	ch, sub := s.Watch([]byte("w/"))
	defer s.CloseWatcher(sub)
	if err := s.Apply(1, []Op{{Key: []byte("w/a"), Value: []byte("1")}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(2, []Op{{Key: []byte("other"), Value: []byte("x")}, {Key: []byte("w/a"), Delete: true}}); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if !bytes.Equal(ev.Key, []byte("w/a")) || ev.Rev != 1 || ev.Delete || string(ev.Value) != "1" {
			t.Fatalf("event1: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no first watch event")
	}
	select {
	case ev := <-ch:
		if !bytes.Equal(ev.Key, []byte("w/a")) || ev.Rev != 2 || !ev.Delete {
			t.Fatalf("event2: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no second watch event")
	}
}

func TestCompactDropsSupersededVersions(t *testing.T) {
	s := newTestStore(t)
	// key a: versions 1,2,3 (3 superseded by 3)
	// key b: version 2, then deleted at 4
	// key c: version 5 only
	ops := []Op{{Key: []byte("a"), Value: []byte("a1")}}
	s.Apply(1, ops)
	s.Apply(2, []Op{{Key: []byte("a"), Value: []byte("a2")}, {Key: []byte("b"), Value: []byte("b2")}})
	s.Apply(3, []Op{{Key: []byte("a"), Value: []byte("a3")}})
	s.Apply(4, []Op{{Key: []byte("b"), Delete: true}})
	s.Apply(5, []Op{{Key: []byte("c"), Value: []byte("c5")}})

	// compact to the latest revision: keeps only the newest version of each key
	dropped, err := s.Compact(5)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 3 {
		t.Fatalf("dropped=%d want 3", dropped)
	}
	// latest reads unchanged
	if v, ok, _ := s.Get(0, []byte("a")); !ok || string(v) != "a3" {
		t.Fatalf("a@latest = %q ok=%v", v, ok)
	}
	if _, ok, _ := s.Get(0, []byte("b")); ok {
		t.Fatal("b should still be deleted at latest")
	}
	if v, ok, _ := s.Get(0, []byte("c")); !ok || string(v) != "c5" {
		t.Fatalf("c@latest = %q ok=%v", v, ok)
	}
	// historical reads at rev >= keep observe the same values
	if v, ok, _ := s.Get(5, []byte("a")); !ok || string(v) != "a3" {
		t.Fatalf("a@5 = %q ok=%v", v, ok)
	}
	if v, ok, _ := s.Get(4, []byte("b")); ok {
		t.Fatalf("b@4 should be deleted, got %q", v)
	}
	// a second compaction is a no-op
	if d, _ := s.Compact(5); d != 0 {
		t.Fatalf("re-compact dropped=%d want 0", d)
	}
}

func TestGrantLease(t *testing.T) {
	s := newTestStore(t)
	l, err := s.GrantLease(3600)
	if err != nil {
		t.Fatal(err)
	}
	if l.Expired() {
		t.Fatal("fresh lease should not be expired")
	}
	if err := s.RevokeLease(l.id); err != nil {
		t.Fatal(err)
	}
	// a fresh lease is never expired
	l2, _ := s.GrantLease(1)
	if l2.Expired() {
		t.Fatal("lease with 1s ttl expired immediately")
	}
	// id is strictly increasing across grants
	if l2.id <= l.id {
		t.Fatalf("lease ids not increasing: %d then %d", l.id, l2.id)
	}
}