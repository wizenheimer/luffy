package main

import (
	"bytes"
	"encoding/binary"
)

// holds key value pair
type Item struct {
	key   []byte
	value []byte
}

type Node struct {
	*dal
	pageNum    pgnum   // node resides on this page
	childNodes []pgnum // holds the child nodes
	items      []*Item // holds list of items i.e. key value pairs
}

func NewEmptyNode() *Node {
	return &Node{}
}

func newItem(key []byte, value []byte) *Item {
	return &Item{
		key:   key,
		value: value,
	}
}

func (n *Node) isLeaf() bool {
	return len(n.childNodes) == 0
}

func (n *Node) serialize(buf []byte) []byte {
	leftPos := 0
	rightPos := len(buf) - 1

	// Add Page Header: isLeaf, kv pair count, node num
	// isLeaf Component
	isLeaf := n.isLeaf()
	var bitSetVar uint64
	if isLeaf {
		bitSetVar = 1
	}
	buf[leftPos] = byte(bitSetVar)
	leftPos += 1

	// kv pair count
	binary.LittleEndian.PutUint16(buf[leftPos:], uint16(len(n.items)))
	leftPos += 2

	for i := 0; i < len(n.items); i += 1 {
		item := n.items[i]

		if !isLeaf {
			childNode := n.childNodes[i]
			binary.LittleEndian.PutUint64(buf[leftPos:], uint64(childNode))
			leftPos += pageNumSize
		}

		klen := len(item.key)
		vlen := len(item.value)

		offset := rightPos - klen - vlen - 2
		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(offset))
		leftPos += 2

		rightPos -= vlen
		copy(buf[rightPos:], item.value)

		rightPos -= 1
		buf[rightPos] = byte(vlen)

		rightPos -= klen
		copy(buf[rightPos:], item.key)

		rightPos -= 1
		buf[rightPos] = byte(klen)
	}

	if !n.isLeaf() {
		lastChildNode := n.childNodes[len(n.childNodes)-1]
		binary.LittleEndian.PutUint64(buf[leftPos:], uint64(lastChildNode))
	}

	return buf
}

func (n *Node) deserialize(buf []byte) {
	leftPos := 0

	isLeaf := uint16(buf[0])

	itemCount := int(binary.LittleEndian.Uint16(buf[1:3]))
	leftPos += 3

	for i := 0; i < itemCount; i += 1 {
		// if not a leaf node
		if isLeaf == 0 {
			pageNum := binary.LittleEndian.Uint64(buf[leftPos:])
			leftPos += pageNumSize

			n.childNodes = append(n.childNodes, pgnum(pageNum))
		}

		// read offset
		offset := binary.LittleEndian.Uint16(buf[leftPos:])
		leftPos += 2

		klen := uint16(buf[int(offset)])
		offset += 1

		key := buf[offset : offset+klen]
		offset += klen

		vlen := uint16(buf[int(offset)])
		offset += 1

		value := buf[offset : offset+vlen]
		offset += vlen

		n.items = append(n.items, newItem(key, value))
	}

	if isLeaf == 0 {
		pageNum := pgnum(binary.LittleEndian.Uint64(buf[leftPos:]))
		n.childNodes = append(n.childNodes, pageNum)
	}
}

func (n *Node) writeNode(node *Node) *Node {
	dnode, _ := n.dal.writeNode(node)
	return dnode
}

func (n *Node) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.writeNode(node)
	}
}

func (n *Node) getNode(pageNum pgnum) (*Node, error) {
	return n.dal.getNode(pageNum)
}

func (n *Node) findKeyInNode(key []byte) (bool, int) {
	for i, existingItem := range n.items {
		res := bytes.Compare(existingItem.key, key)
		if res == 0 {
			return true, i
		}

		if res == 1 {
			return false, i
		}
	}
	return false, len(n.items)
}

func (n *Node) findKey(key []byte) (int, *Node, error) {
	index, node, err := findKeyHelper(n, key)
	if err != nil {
		return -1, nil, err
	}

	return index, node, nil
}

func findKeyHelper(node *Node, key []byte) (int, *Node, error) {
	wasFound, index := node.findKeyInNode(key)
	if wasFound {
		return index, node, nil
	}
	if node.isLeaf() {
		return -1, nil, nil
	}
	nextChild, err := node.getNode(node.childNodes[index])
	if err != nil {
		return -1, nil, err
	}

	return findKeyHelper(nextChild, key)
}

// calculates the size of the element in a node
func (n *Node) elementSize(i int) int {
	size := 0
	size += len(n.items[i].key)
	size += len(n.items[i].value)
	size += pageNumSize
	return size
}

// calculate the size of node along with the elements in it
func (n *Node) nodeSize() int {
	size := 0
	size += nodeHeaderSize
	for i := range n.items {
		size += n.elementSize(i)
	}
	size += pageNumSize
	return size
}

// add item into the node
func (n *Node) addItem(item *Item, insertionIndex int) int {
	if len(n.items) == insertionIndex {
		n.items = append(n.items, item)
		return insertionIndex
	}
	n.items = append(n.items[:insertionIndex], n.items[insertionIndex:]...)
	n.items[insertionIndex] = item
	return insertionIndex
}

// determine whether node is overpopulated or not
func (n *Node) isOverPopulated() bool {
	return n.dal.isOverPopulated(n)
}

// determine whether node is underpopulated or not
func (n *Node) isUnderPopulated() bool {
	return n.dal.isUnderPopulated(n)
}

// split the nodes
func (n *Node) split(nodeToSplit *Node, nodeToSplitIndex int) {
	splitIndex := nodeToSplit.dal.getSplitIndex(nodeToSplit)

	middleItem := nodeToSplit.items[splitIndex]
	var newNode *Node

	if nodeToSplit.isLeaf() {
		newNode = n.writeNode(n.dal.newNode(nodeToSplit.items[splitIndex+1:], []pgnum{}))
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
	} else {
		newNode = n.writeNode(n.dal.newNode(nodeToSplit.items[splitIndex+1:], nodeToSplit.childNodes[splitIndex+1:]))
		nodeToSplit.items = nodeToSplit.items[:splitIndex]
		nodeToSplit.childNodes = nodeToSplit.childNodes[:splitIndex+1]
	}

	n.addItem(middleItem, nodeToSplitIndex)

	if len(n.childNodes) == nodeToSplitIndex+1 {
		n.childNodes = append(n.childNodes, newNode.pageNum)
	} else {
		n.childNodes = append(n.childNodes[:nodeToSplitIndex+1], n.childNodes[nodeToSplitIndex:]...)
		n.childNodes[nodeToSplitIndex+1] = newNode.pageNum
	}

	n.writeNodes(n, nodeToSplit)
}
