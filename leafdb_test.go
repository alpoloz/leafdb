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

func TestPersistence(t *testing.T) {
	path := t.TempDir() + "/leaf.db"

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if err := db.Set([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := db.Set([]byte("k2"), []byte("v2")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db.Close()

	val, ok := db.Get([]byte("k1"))
	if !ok || string(val) != "v1" {
		t.Fatalf("expected v1, got %q", val)
	}
}

func TestBufferedFlushOnClose(t *testing.T) {
	path := t.TempDir() + "/leaf-buffered.db"

	db, err := OpenWithOptions(path, &Options{FlushEvery: 3})
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	if err := db.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := db.Set([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	db, err = Open(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer db.Close()

	val, ok := db.Get([]byte("b"))
	if !ok || string(val) != "2" {
		t.Fatalf("expected 2, got %q", val)
	}
}

func TestFreeListReuse(t *testing.T) {
	path := t.TempDir() + "/leaf-free.db"

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	for i := 0; i < 20; i++ {
		key := []byte{byte('a' + i)}
		if err := db.Set(key, []byte{byte('A' + i)}); err != nil {
			t.Fatalf("set failed: %v", err)
		}
	}

	root := db.index.root
	if root.isLeaf || len(root.children) < 2 {
		t.Fatalf("expected internal root with two children")
	}
	rightLeaf := root.children[1]
	freedID := rightLeaf.pageID

	for i := 8; i < 20; i++ {
		key := []byte{byte('a' + i)}
		removed, err := db.Delete(key)
		if err != nil {
			t.Fatalf("delete failed: %v", err)
		}
		if !removed {
			t.Fatalf("expected key removal for %q", key)
		}
	}

	if !containsPageID(db.pager.freeList, freedID) {
		t.Fatalf("expected freed page id to be in freelist")
	}

	// Force multiple splits to consume freed pages.
	for i := 0; i < 100; i++ {
		if err := db.Set([]byte{byte('k' + i)}, []byte{byte('K' + i)}); err != nil {
			t.Fatalf("set failed: %v", err)
		}
	}

	if containsPageID(db.pager.freeList, freedID) {
		t.Fatalf("expected freed page id to be reused")
	}
	if !treeHasPageID(db.index.root, freedID) {
		t.Fatalf("expected freed page id to be allocated to a node")
	}
}

func TestTransactionCommit(t *testing.T) {
	db := New()
	tx := db.Begin(true)
	if err := tx.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := tx.Set([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	val, ok := tx.Get([]byte("a"))
	if !ok || string(val) != "1" {
		t.Fatalf("expected read-your-writes")
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	got, ok := db.Get([]byte("b"))
	if !ok || string(got) != "2" {
		t.Fatalf("expected committed value")
	}
}

func TestTransactionRollback(t *testing.T) {
	db := New()
	tx := db.Begin(true)
	if err := tx.Set([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	tx.Rollback()

	if _, ok := db.Get([]byte("a")); ok {
		t.Fatalf("expected rollback to discard changes")
	}
}

func TestViewUpdateHelpers(t *testing.T) {
	db := New()
	if err := db.Update(func(tx *Tx) error {
		return tx.Set([]byte("k"), []byte("v"))
	}); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if err := db.View(func(tx *Tx) error {
		val, ok := tx.Get([]byte("k"))
		if !ok || string(val) != "v" {
			t.Fatalf("expected value in view")
		}
		return nil
	}); err != nil {
		t.Fatalf("view failed: %v", err)
	}
}

func TestReadSnapshotIsolation(t *testing.T) {
	db := New()
	if err := db.Set([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	tx := db.Begin(false)
	if err := db.Set([]byte("k"), []byte("v2")); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	val, ok := tx.Get([]byte("k"))
	if !ok || string(val) != "v1" {
		t.Fatalf("expected snapshot value v1, got %q", val)
	}
	tx.Rollback()
}

func containsPageID(ids []uint64, id uint64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

func treeHasPageID(n *bpnode, id uint64) bool {
	if n == nil {
		return false
	}
	if n.pageID == id {
		return true
	}
	for _, child := range n.children {
		if treeHasPageID(child, id) {
			return true
		}
	}
	return false
}
