package luffy

import "encoding/binary"

const (
	metaPage = 0 // highest page number in use by the database
)

type freelist struct {
	maxPage       pgnum   // highest page number allocated yet
	releasedPages []pgnum // pages that have been allocated previously but now released
}

func newFreelist() *freelist {
	return &freelist{
		maxPage:       metaPage,
		releasedPages: []pgnum{},
	}
}

// returns next page available tracked by freelist
// incase, there isn't increment the page number
func (f *freelist) getNextPage() pgnum {
	if len(f.releasedPages) != 0 {
		pageID := f.releasedPages[len(f.releasedPages)-1]
		f.releasedPages = f.releasedPages[:len(f.releasedPages)-1]
		return pageID
	}
	f.maxPage += 1
	return f.maxPage
}

// appends released page into freelist
func (f *freelist) releasePage(page pgnum) {
	f.releasedPages = append(f.releasedPages, page)
}

// serializes 1/ maximum pagenumber allocated yet
// serializes 2/ count of released pages
// serializes 3/ the actual pages
func (f *freelist) serialize(buf []byte) []byte {
	pos := 0

	binary.LittleEndian.PutUint16(buf[pos:], uint16(f.maxPage))
	pos += 2

	binary.LittleEndian.PutUint16(buf[pos:], uint16(len(f.releasedPages)))
	pos += 2

	for _, page := range f.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumSize
	}

	return buf
}

// de-serializes 1/ maximum pagenumber allocated yet
// de-serializes 2/ count of released pages
// de-serializes 3/ the actual pages
func (f *freelist) deserialize(buf []byte) {
	pos := 0
	f.maxPage = pgnum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	releasedPagesCount := int(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2

	for i := 0; i < releasedPagesCount; i += 1 {
		f.releasedPages = append(f.releasedPages, pgnum(binary.LittleEndian.Uint64(buf[pos:])))
		pos += pageNumSize
	}
}
