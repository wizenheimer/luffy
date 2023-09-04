package main

import "encoding/binary"

const (
	metaPageNum = 0 // denotes the page number of the meta page, holding the metadata about the db
)

type meta struct {
	freelistPage pgnum
}

func newEmptyMeta() *meta {
	return &meta{}
}

// serialize and load the freelist page
func (m *meta) serialize(buf []byte) {
	pos := 0
	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freelistPage))
	pos += pageNumSize
}

// deserialize and store the freelistPage onto the struct
func (m *meta) deserialize(buf []byte) {
	pos := 0
	m.freelistPage = pgnum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize
}
