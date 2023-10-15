package godb

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
	//add additional fields here, if needed
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child: child, limitTups: lim}
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iterator, _ := l.child.Iterator(tid)
	lim_dbval, _ := l.limitTups.EvalExpr(nil)
	lim := int(lim_dbval.(IntField).Value)
	i := 0
	return func() (*Tuple, error) {
		for i < lim {
			t, _ := iterator()
			if t == nil {
				break
			}
			i += 1
			return t, nil
		}
		return nil, nil
	}, nil
}
