package godb

import "fmt"

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	project := &Project{
		selectFields: selectFields,
		outputNames:  outputNames,
		child:        child,
	}
	return project, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	fts := []FieldType{}
	for i, field := range p.selectFields {
		ft := field.GetExprType()
		ft.Fname = p.outputNames[i]
		fts = append(fts, ft)
	}
	td := TupleDesc{}
	td.Fields = fts
	return &td

}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	childIterator, _ := p.child.Iterator(tid)
	return func() (*Tuple, error) {
		t, _ := childIterator()
		if t == nil {
			return nil, nil
		}
		fields := []DBValue{}
		for _, selectField := range p.selectFields {
			val, _ := selectField.EvalExpr(t)
			fields = append(fields, val)
		}
		td := &Tuple{
			Desc:   *p.Descriptor(),
			Fields: fields,
		}
		fmt.Println(*td)
		return td, nil
	}, nil
}
