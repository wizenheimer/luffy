package luffy

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

// conversion of structs to binary and put it into freelistpage
func (m *meta) serialize(buf []byte) {
	pos := 0
	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.freelistPage))
	pos += pageNumSize
}

// deserialize the bytes
func (m *meta) deserialize(buf []byte) {
	pos := 0
	binary.LittleEndian.Uint64(buf[pos:])
	pos += pageNumSize
}
