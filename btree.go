package luffy

import "encoding/binary"

const (
	BNODE_NODE         = 1
	BNODE_LEAF         = 2
	HEADER             = 4
	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

type (
	BTree struct {
		root uint64
		get  func(uint64) BNode
		new  func(BNode) uint64
		del  func(uint64)
	}

	BNode struct {
		/*
			| type | nkeys | pointers | offsets | key-values
			| 2B | 2B | nkeys * 8B | nkeys * 2B | ...> format of node

			| klen | vlen | key | val |
			| 2B | 2B | ... | ... | ..> format of the KV pair. Lengths followed by data.

		*/
		data []byte
	}
)

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

func (node BNode) getPtr(index uint16) uint64 {
	if index < node.nkeys() {
		panic("index can't be resolved")
	}
	pos := HEADER + 8*index
	return binary.LittleEndian.Uint64(node.data[pos:])
}

func (node BNode) setPtr(index uint16, value uint64) {
	if index < node.nkeys() {
		panic("index can't be resolved")
	}
	pos := HEADER + 8*index
	binary.LittleEndian.PutUint64(node.data[pos:], value)
}

func offsetPos(node BNode, index uint16) uint16 {
	if index < 1 && index > node.nkeys() {
		panic("offset can't be resolved")
	}
	return HEADER + 8*node.nkeys() + 2*(index-1)
}

func (node BNode) getOffset(index uint16) uint16 {
	if index == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, index):])
}

func (node BNode) setOffset(index uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, index):], offset)
}

func (node BNode) kvPos(index uint16) uint16 {
	if index > node.nkeys() {
		panic("index can't be resolved")
	}
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(index)
}

func (node BNode) getKey(index uint16) []byte {
	if index >= node.nkeys() {
		panic("index can't be resolved")
	}
	pos := node.kvPos(index)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}

func (node BNode) getVal(index uint16) []byte {
	if index >= node.nkeys() {
		panic("index can't be resolved")
	}
	pos := node.kvPos(index)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE
	if node1max > BTREE_PAGE_SIZE {
		panic("node exceeds BTREE_PAGE_SIZE")
	}
}
