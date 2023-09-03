package luffy

const metaPage = 0 // highest page number in use by the database

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
