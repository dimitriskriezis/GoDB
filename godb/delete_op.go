package godb

type DeleteOp struct {
	file  DBFile
	child Operator
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{file: deleteFile, child: child}
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	ft := FieldType{"count", "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID, desc *TupleDesc) (func() (*Tuple, error), error) {
	iterator, _ := dop.child.Iterator(tid, dop.Descriptor())
	count := 0
	return func() (*Tuple, error) {
		for {
			t, _ := iterator()
			if t == nil {
				break
			}
			deleteError := dop.file.deleteTuple(t, tid)
			if deleteError != nil {
				return nil, deleteError
			}
			count += 1
		}
		return &Tuple{Desc: *dop.Descriptor(), Fields: []DBValue{IntField{Value: int64(count)}}}, nil
	}, nil

}
