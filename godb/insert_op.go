package godb

// TODO: some code goes here
type InsertOp struct {
	file  DBFile
	child Operator
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{file: insertFile, child: child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	ft := FieldType{"count", "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iterator, _ := iop.child.Iterator(tid)
	count := 0
	return func() (*Tuple, error) {
		for {
			t, _ := iterator()
			if t == nil {
				break
			}
			iop.file.insertTuple(t, tid)
			// if insertError != nil {
			// 	return nil, insertError
			// }
			count += 1
		}
		return &Tuple{Desc: *iop.Descriptor(), Fields: []DBValue{IntField{Value: int64(count)}}}, nil
	}, nil
}
