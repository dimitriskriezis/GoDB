package godb

import (
	"fmt"
	"testing"
)

func TestGetPage(t *testing.T) {
	_, t1, t2, hf, bp, _ := makeTestVars()
	tid := NewTID()
	for i := 0; i < 300; i++ {
		fmt.Println("Check status of pages")
		for _, val := range bp.Pages {
			fmt.Println(val, (*val).isDirty())
		}
		bp.BeginTransaction(tid)
		err := hf.insertTuple(&t1, tid)
		if err != nil {
			t.Fatalf("%v", err)
		}
		fmt.Println("Check status of pages after first tuple insertion")
		for _, val := range bp.Pages {
			fmt.Println(val, (*val).isDirty())
		}
		fmt.Println(len(bp.Pages))
		err = hf.insertTuple(&t2, tid)
		fmt.Println(i)
		if err != nil {
			t.Fatalf("%v", err)
		}
		bp.CommitTransaction(tid)
		//hack to force dirty pages to disk
		//because CommitTransaction may not be implemented
		//yet if this is called in lab 1
		for i := 0; i < 6; i++ {
			fmt.Println("In test ", i)
			pg, err := bp.GetPage(hf, i, tid, ReadPerm)
			fmt.Println("In test ", i)
			if pg == nil || err != nil {
				break
			}
			if (*pg).isDirty() {
				(*(*pg).getFile()).flushPage(pg)
				(*pg).setDirty(false)
			}
			fmt.Println("Iteration ", i)
		}
		fmt.Println("Check status of pages after them being clean")
		for _, val := range bp.Pages {
			fmt.Println(val, (*val).isDirty())
		}

	}
	fmt.Println("How about here")
	bp.BeginTransaction(tid)
	//expect 6 pages

	for i := 0; i < 6; i++ {
		fmt.Println("Num pages in the file", hf.numPages)
		pg, err := bp.GetPage(hf, i, tid, ReadPerm)
		if pg == nil || err != nil {
			t.Fatalf("failed to get page %d (err = %v)", i, err)
		}
	}
	_, err := bp.GetPage(hf, 7, tid, ReadPerm)
	if err == nil {
		t.Fatalf("expected to get page 7 but failed")
	}

}
