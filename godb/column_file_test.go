package godb

import (
	"os"
	"testing"
)

func makeCFTestVars() (TupleDesc, Tuple, Tuple, *ColumnFile, *BufferPool, TransactionID) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}

	var t1 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
		}}

	var t2 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"george jones"},
			IntField{999},
		}}

	bp := NewBufferPool(3)
	for _, field := range td.Fields {
		os.Remove(TestingFile + "_" + field.Fname + ".dat")
	}
	cf, err := NewColumnFile(TestingFile, &td, bp)
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err)
	}

	tid := NewTID()
	bp.BeginTransaction(tid)

	return td, t1, t2, cf, bp, tid

}

func TestCreateAndInsertColumnFile(t *testing.T) {
	_, t1, t2, cf, _, tid := makeCFTestVars()
	cf.insertTuple(&t1, tid)
	cf.insertTuple(&t2, tid)
	iter, _ := cf.Iterator(tid, cf.Descriptor())
	i := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		i = i + 1
	}
	if i != 2 {
		t.Errorf("HeapFile iterator expected 2 tuples, got %d", i)
	}
}

func TestDeleteColumnFile(t *testing.T) {
	_, t1, t2, cf, _, tid := makeCFTestVars()
	cf.insertTuple(&t1, tid)
	cf.insertTuple(&t2, tid)

	cf.deleteTuple(&t1, tid)
	iter, _ := cf.Iterator(tid, cf.Descriptor())
	t3, _ := iter()
	if t3 == nil {
		t.Errorf("HeapFile iterator expected 1 tuple")
	}
	cf.deleteTuple(&t2, tid)
	iter, _ = cf.Iterator(tid, cf.Descriptor())
	t3, _ = iter()
	if t3 != nil {
		t.Errorf("HeapFile iterator expected 0 tuple")
	}
}

func testSerializeColumnFileN(t *testing.T, n int) {
	td, t1, t2, cf, bp, _ := makeCFTestVars()
	for i := 0; i < n; i++ {
		tid := NewTID()
		bp.BeginTransaction(tid)
		err := cf.insertTuple(&t1, tid)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		err = cf.insertTuple(&t2, tid)
		if err != nil {
			t.Errorf(err.Error())
			return
		}

		//commit frequently to prevent buffer pool from filling
		//todo fix
		bp.CommitTransaction(tid)

	}
	bp.FlushAllPages()
	bp2 := NewBufferPool(1)
	cf2, _ := NewColumnFile(TestingFile, &td, bp2)
	tid := NewTID()
	bp2.BeginTransaction(tid)
	iter, _ := cf2.Iterator(tid, cf2.Descriptor())
	i := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		i = i + 1
	}
	if i != 2*n {
		t.Errorf("HeapFile iterator expected %d tuples, got %d", 2*n, i)
	}
}

func TestSerializeSmallColumnFile(t *testing.T) {
	testSerializeColumnFileN(t, 2)
}

func TestSerializeLargeColumnFile(t *testing.T) {
	testSerializeColumnFileN(t, 2000)
}

func TestSerializeVeryLargeColumnFile(t *testing.T) {
	testSerializeColumnFileN(t, 4000)
}

func TestLoadCSVColumnFile(t *testing.T) {
	_, _, _, cf, _, tid := makeCFTestVars()
	f, err := os.Open("test_heap_file.csv")
	if err != nil {
		t.Errorf("Couldn't open test_heap_file.csv")
		return
	}
	err = cf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		t.Fatalf("Load failed, %s", err)
	}

	//should have 384 records
	iter, _ := cf.Iterator(tid, cf.Descriptor())
	i := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		i = i + 1
	}
	if i != 384 {
		t.Errorf("HeapFile iterator expected 384 tuples, got %d", i)
	}
}

func TestReadTwoColumn(t *testing.T) {
	td := TupleDesc{Fields: []FieldType{
		{Fname: "route_id", Ftype: IntType},
		{Fname: "line_id", Ftype: StringType},
		{Fname: "first_station_id", Ftype: StringType},
		{Fname: "last_station_id", Ftype: StringType},
		{Fname: "direction", Ftype: IntType},
		{Fname: "direction_desc", Ftype: StringType},
		{Fname: "route_name", Ftype: StringType}}}

	bp := NewBufferPool(20)
	cf, _ := NewColumnFile("transitdb/routes.dat", &td, bp)

	selectDesc := TupleDesc{Fields: []FieldType{
		{Fname: "first_station_id", Ftype: StringType},
		{Fname: "last_station_id", Ftype: StringType},
	}}
	iter, _ := cf.Iterator(NewTID(), &selectDesc)
	i := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		if len(tup.Fields) != 2 {
			t.Errorf("ColumnFile expected 2 columns got %d", len(tup.Fields))
		}
		i = i + 1
	}
	if i != 18 {
		t.Errorf("HeapFile iterator expected 18 tuples, got %d", i)
	}
}

func TestEachColumnHasOneTuple(t *testing.T) {
	td := TupleDesc{Fields: []FieldType{
		{Fname: "route_id", Ftype: IntType},
	}}

	bp := NewBufferPool(20)
	hf, _ := NewHeapFile("transitdb/routes.dat_route_id.dat", &td, bp)
	iter, _ := hf.Iterator(NewTID(), &td)
	i := 0
	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		if len(tup.Fields) != 1 {
			t.Errorf("ColumnFile expected 1 columns got %d", len(tup.Fields))
		}
		i = i + 1
	}
	if i != 18 {
		t.Errorf("HeapFile iterator expected 18 tuples, got %d", i)
	}
}
