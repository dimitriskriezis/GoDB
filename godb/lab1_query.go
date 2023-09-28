package godb

import (
	"os"
)

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	tid := NewTID()
	bp := NewBufferPool(3)
	bp.BeginTransaction(tid)
	os.Remove("lab1_query.dat")
	f, initError := NewHeapFile("lab1_query.dat", &td, bp)
	if initError != nil {
		return 0, initError
	}
	file, csvOpenError := os.Open(fileName)
	if csvOpenError != nil {
		return 0, csvOpenError
	}
	loadCsvError := f.LoadFromCSV(file, true, ",", false)
	if loadCsvError != nil {
		return 0, loadCsvError
	}
	iterator, _ := f.Iterator(tid)
	sum := 0
	for {
		t, _ := iterator()
		if t == nil {
			break
		}
		IdIndex, indexError := findFieldInTd(FieldType{Fname: sumField}, &td)
		if indexError != nil {
			return 0, indexError
		}
		field_value := t.Fields[IdIndex]
		fieldValue := field_value.(IntField)
		sum += (int)(fieldValue.Value)
	}

	return sum, nil // replace me
}
