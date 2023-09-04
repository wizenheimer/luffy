package main

import (
	"errors"
	"os"
)

type (
	// for holding page number of the page
	pgnum uint64

	// for representing data access layer
	// dal.pageSize holds the capacity of the page
	// dal.freelist embeds the freelist struct
	// dal.meta embeds the meta struct
	dal struct {
		file     *os.File
		pageSize int
		*freelist
		*meta
	}

	// for representing page
	// page.num hold the page number identifier
	// page.data hold the page number data
	page struct {
		num  pgnum
		data []byte
	}
)

// constructor for creating a data access layer
func newDal(path string) (*dal, error) {
	dal := &dal{
		meta:     newEmptyMeta(),
		pageSize: os.Getpagesize(),
	}

	// file already exists at the path, read it and load it into struct
	if _, err := os.Stat(path); err == nil {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}

		meta, err := dal.readMeta()
		if err != nil {
			return nil, err
		}
		dal.meta = meta

		freelist, err := dal.readFreelist()
		if err != nil {
			return nil, err
		}
		dal.freelist = freelist
		// doesn't exist
	} else if errors.Is(err, os.ErrNotExist) {
		// init freelist
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.close()
			return nil, err
		}

		dal.freelist = newFreelist()
		dal.freelistPage = dal.getNextPage()
		_, err := dal.writeFreelist()
		if err != nil {
			return nil, err
		}

		// write meta page
		_, err = dal.writeMeta(dal.meta) // other error
		if err != nil {
			return nil, err
		}
	} else {
		return nil, err
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

// serialize and persist meta onto memory
func (d *dal) writeMeta(m *meta) (*page, error) {
	p := d.allocateEmptyPage()
	p.num = metaPageNum
	m.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// deserialize
func (d *dal) readMeta() (*meta, error) {
	p, err := d.readPage(metaPageNum)
	if err != nil {
		return nil, err
	}

	meta := newEmptyMeta()
	meta.deserialize(p.data)
	return meta, nil
}

// writing freelists onto memory
// allocates an empty page
// assigns the highest allocated page to freelistpage
// serializes the freelist onto the page
func (d *dal) writeFreelist() (*page, error) {
	p := d.allocateEmptyPage()
	p.num = d.freelistPage
	d.freelist.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}

	d.freelistPage = p.num
	return p, nil
}

// read the freelist page
// deserialize and load onto a new freelist struct
func (d *dal) readFreelist() (*freelist, error) {
	p, err := d.readPage(d.freelistPage)
	if err != nil {
		return nil, err
	}

	freelist := newFreelist()
	freelist.deserialize(p.data)
	return freelist, nil
}

// fetch node from page
func (d *dal) getNode(pageNum pgnum) (*Node, error) {
	p, err := d.readPage(pageNum)
	if err != nil {
		return nil, err
	}

	node := NewEmptyNode()
	node.deserialize(p.data)
	node.pageNum = pageNum
	return node, nil
}

// write node into page
func (d *dal) writeNode(n *Node) (*Node, error) {
	p := d.allocateEmptyPage()
	if n.pageNum == 0 {
		p.num = d.getNextPage()
		n.pageNum = p.num
	} else {
		p.num = n.pageNum
	}

	p.data = n.serialize(p.data)

	err := d.writePage(p)
	if err != nil {
		return nil, err
	}

	return n, nil
}

func (d *dal) deleteNode(pageNum pgnum) {
	d.releasePage(pageNum)
}
