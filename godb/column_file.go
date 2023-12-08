package godb

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ColumnFile represents a file that stores a specific column
// Each column file has a descriptor desc of all the columns it contains
// Each column file has an array of all the heapfiles that represent a column in the table
type ColumnFile struct {
	name        string
	Desc        *TupleDesc
	ColumnFiles []*HeapFile
	bufPool     *BufferPool
}

func NewColumnFile(name string, td *TupleDesc, bp *BufferPool) (*ColumnFile, error) {
	// make a new column file from
	heapFiles := []*HeapFile{}
	for i := range td.Fields {
		columnTd := &TupleDesc{Fields: []FieldType{td.Fields[i]}}
		fromFile := name + "_" + td.Fields[i].Fname + ".dat"
		columnHeapFile, _ := NewHeapFile(fromFile, columnTd, bp)
		heapFiles = append(heapFiles, columnHeapFile)
	}
	return &ColumnFile{
		name:        name,
		Desc:        td,
		ColumnFiles: heapFiles,
		bufPool:     bp,
	}, nil
}

// insertTuple
func (cf *ColumnFile) insertTuple(t *Tuple, tid TransactionID) error {
	for i := range t.Fields {
		fieldTuple := &Tuple{
			Desc:   TupleDesc{Fields: []FieldType{t.Desc.Fields[i]}},
			Fields: []DBValue{t.Fields[i]},
		}
		err := cf.ColumnFiles[i].insertTuple(fieldTuple, tid)
		if err != nil {
			return err
		}
		t.Rid = fieldTuple.Rid
	}
	return nil
}

// Delete tuple
func (cf *ColumnFile) deleteTuple(t *Tuple, tid TransactionID) error {
	for i := range t.Fields {
		fieldTuple := &Tuple{
			Desc:   TupleDesc{Fields: []FieldType{t.Desc.Fields[i]}},
			Fields: []DBValue{t.Fields[i]},
			Rid:    t.Rid,
		}
		err := cf.ColumnFiles[i].deleteTuple(fieldTuple, tid)
		if err != nil {
			return err
		}
	}
	return nil
}

// Iterator for early materialization
func (cf *ColumnFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iterators := []func() (*Tuple, error){}
	for _, f := range cf.ColumnFiles {
		it, _ := f.Iterator(tid)
		iterators = append(iterators, it)
	}
	return func() (*Tuple, error) {
		tuple := &Tuple{
			Desc:   TupleDesc{Fields: []FieldType{}},
			Fields: []DBValue{},
		}
		for _, iter := range iterators {
			t, err := iter()
			if err != nil {
				return nil, err
			}
			if t == nil {
				return nil, nil
			}
			tuple = joinTuples(tuple, t)
			tuple.Rid = t.Rid
		}
		// if I can get here I will
		return tuple, nil
	}, nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (cf *ColumnFile) Descriptor() *TupleDesc {
	return cf.Desc
}

func (cf *ColumnFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := cf.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(cf.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch cf.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*cf.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := cf.bufPool
		bp.BeginTransaction(tid)
		cf.insertTuple(&newT, tid)

		bp.CommitTransaction(tid)
	}
	return nil
}

func (cf *ColumnFile) flushPage(p *Page) error {
	return nil
}

func (f *ColumnFile) pageKey(pgNo int) any {
	return nil
}

func (f *ColumnFile) readPage(pageNo int) (*Page, error) {
	return nil, nil
}
