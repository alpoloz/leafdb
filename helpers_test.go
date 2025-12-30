package leafdb

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
