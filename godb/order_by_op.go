package godb

import (
	"sort"
)

// TODO: some code goes here
type OrderBy struct {
	orderBy   []Expr // OrderBy should include these two fields (used by parser)
	child     Operator
	ascending []bool
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	return &OrderBy{
		orderBy:   orderByFields,
		child:     child,
		ascending: ascending,
	}, nil
}

func (o *OrderBy) Descriptor() *TupleDesc {
	fts := []FieldType{}
	for _, field := range o.orderBy {
		ft := field.GetExprType()
		fts = append(fts, ft)
	}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

type lessFunc func(field Expr, p1, p2 *Tuple) bool

type multiSorter struct {
	tuples        []*Tuple
	less          []lessFunc
	OrderByFields []Expr
}

func (ms *multiSorter) Sort(tuples []*Tuple) {
	ms.tuples = tuples
	sort.Sort(ms)
}
func OrderedBy(orderByFields []Expr, less ...lessFunc) *multiSorter {
	return &multiSorter{
		less:          less,
		OrderByFields: orderByFields,
	}
}
func (ms *multiSorter) Len() int {
	return len(ms.tuples)
}

func (ms *multiSorter) Swap(i, j int) {
	ms.tuples[i], ms.tuples[j] = ms.tuples[j], ms.tuples[i]
}

func (ms *multiSorter) Less(i, j int) bool {
	p, q := ms.tuples[i], ms.tuples[j]
	// Try all but the last comparison.
	var k int
	for k = 0; k < len(ms.less)-1; k++ {
		less := ms.less[k]
		switch {
		case less(ms.OrderByFields[k], p, q):
			// p < q, so we have a decision.
			return true
		case less(ms.OrderByFields[k], q, p):
			// p > q, so we have a decision.
			return false
		}
		// p == q; try the next comparison.
	}
	// All comparisons to here said "equal", so just return whatever
	// the final comparison reports.
	return ms.less[k](ms.OrderByFields[k], p, q)
}

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIterator, _ := o.child.Iterator(tid)
	tuples := []*Tuple{}
	for {
		t, _ := childIterator()
		if t == nil {
			break
		}
		tuples = append(tuples, t)
	}
	lessthan := func(field Expr, t1, t2 *Tuple) bool {
		dbval1, _ := field.EvalExpr(t1)
		dbval2, _ := field.EvalExpr(t2)
		switch field.GetExprType().Ftype {
		case IntType:
			val1 := dbval1.(IntField)
			val2 := dbval2.(IntField)
			return val1.Value < val2.Value

		case StringType:
			val1 := dbval1.(StringField)
			val2 := dbval2.(StringField)
			return val1.Value < val2.Value
		}
		return false
	}
	greaterthan := func(field Expr, t1, t2 *Tuple) bool {
		dbval1, _ := field.EvalExpr(t1)
		dbval2, _ := field.EvalExpr(t2)
		switch field.GetExprType().Ftype {
		case IntType:
			val1 := dbval1.(IntField)
			val2 := dbval2.(IntField)
			return val1.Value > val2.Value

		case StringType:
			val1 := dbval1.(StringField)
			val2 := dbval2.(StringField)
			return val1.Value > val2.Value
		}
		return false
	}
	orderFuncs := []lessFunc{}
	for _, val := range o.ascending {
		if val {
			orderFuncs = append(orderFuncs, lessthan)
		} else {
			orderFuncs = append(orderFuncs, greaterthan)
		}
	}
	OrderedBy(o.orderBy, orderFuncs...).Sort(tuples)
	counter := 0
	return func() (*Tuple, error) {
		for counter < len(tuples) {
			returnVal := tuples[counter]
			counter += 1
			return returnVal, nil
		}
		return nil, nil
	}, nil
}
