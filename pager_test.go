package leafdb

import "testing"

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
