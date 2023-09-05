package main

import "errors"

const (
	pageNumSize    = 8 // denotes the size of the page in bytes
	nodeHeaderSize = 3
)

var (
	writeInsideReadTxErr = errors.New("can't perform a write operation inside a read transaction")
)
