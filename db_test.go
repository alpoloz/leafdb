package leafdb

import "testing"

func TestSetGet(t *testing.T) {
	db := New()
	key := []byte("alpha")
	val := []byte("one")

	if err := db.Set(key, val); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	got, ok := db.Get(key)
	if !ok {
		t.Fatalf("expected key to exist")
	}
	if string(got) != "one" {
		t.Fatalf("unexpected value: %s", got)
	}

	if err := db.Set(key, []byte("two")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	got, ok = db.Get(key)
	if !ok || string(got) != "two" {
		t.Fatalf("expected updated value, got %q", got)
	}
}

func TestSplit(t *testing.T) {
	db := &DB{index: newBPTree(4, nil)}
	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i)}
		if err := db.Set(key, []byte{byte('A' + i)}); err != nil {
			t.Fatalf("set failed: %v", err)
		}
	}
	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i)}
		got, ok := db.Get(key)
		if !ok || len(got) != 1 || got[0] != byte('A'+i) {
			t.Fatalf("missing or wrong value for %q", key)
		}
	}
}
