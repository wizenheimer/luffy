package luffy

import "os"

type (
	// for holding page number of the page
	pgnum uint64

	// for representing data access layer
	dal struct {
		file     *os.File
		pageSize int
	}

	// for representing page
	page struct {
		num  pgnum
		data []byte
	}
)

// constructor for creating a data access layer
func newDal(path string, pageSize int) (*dal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	dal := &dal{
		file:     file,
		pageSize: pageSize,
	}

	return dal, nil
}

// helper to close the file
func (d *dal) close() error {
	if d.file != nil {
		err := d.file.Close()

		if err != nil {
			return err
		}

		d.file = nil
	}

	return nil
}

// helper to allocate memory equal to the pagesize, used to load the contents of page into the datastructure
func (d *dal) allocateEmptyPage() *page {
	return &page{
		data: make([]byte, d.pageSize),
	}
}

// helper to read page
func (d *dal) readPage(n pgnum) (*page, error) {
	page := d.allocateEmptyPage()

	// read and load data from the given byte offset
	offset := int(n) * d.pageSize
	_, err := d.file.ReadAt(page.data, int64(offset))

	if err != nil {
		return nil, err
	}

	return page, nil
}

// helper to write page data onto memory
func (d *dal) writePage(p *page) error {
	// persist page data onto memory
	offset := int64(p.num) * int64(d.pageSize)
	_, err := d.file.WriteAt(p.data, offset)
	return err
}
