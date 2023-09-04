package luffy

import "os"

func _() {
	dal, _ := newDal("test.db", os.Getpagesize())
	p := dal.allocateEmptyPage()
	p.num = dal.getNextPage()
	copy(p.data[:], "lorem-ipsum")
	_ = dal.writePage(p)
}
