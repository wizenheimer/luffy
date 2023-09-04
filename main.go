package main

func main() {
	// iniialize db
	dal, _ := newDal("test.db")

	// create new page
	p := dal.allocateEmptyPage()
	p.num = dal.getNextPage()
	copy(p.data[:], "lorem-ipsum")

	// commit page onto disk
	_ = dal.writePage(p)
	_, _ = dal.writeFreelist()

	// close db
	_ = dal.close()

	// iniialize db
	dal, _ = newDal("test.db")

	// create new page
	p = dal.allocateEmptyPage()
	p.num = dal.getNextPage()
	copy(p.data[:], "dolorem-ipsum")

	// commit page onto disk
	_ = dal.writePage(p)
	_, _ = dal.writeFreelist()

	// close db
	_ = dal.close()
}
