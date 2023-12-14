package godb

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Function to make validation database
func MakeValidationDataset() error {
	td := TupleDesc{Fields: []FieldType{}}
	for i := 0; i < 500; i++ {
		td.Fields = append(td.Fields, FieldType{Fname: "column_" + strconv.Itoa(i)})
	}

	cfbp := NewBufferPool(20000)
	hfbp := NewBufferPool(20000)
	cf, _ := NewColumnFile("godb/validation/columntable500.dat", &td, cfbp)
	hf, _ := NewHeapFile("godb/validation/columntable500.dat", &td, hfbp)
	cfid := NewTID()
	hfid := NewTID()
	for i := 0; i < 10000; i++ {
		println(i)
		tup := &Tuple{Desc: td, Fields: []DBValue{}}
		for j := 0; j < 500; j++ {
			tup.Fields = append(tup.Fields, IntField{int64(j)})
		}
		errf := cf.insertTuple(tup, cfid)
		if errf != nil {
			return errf
		}
		errh := hf.insertTuple(tup, hfid)
		if errh != nil {
			return errh
		}
		cf.bufPool.FlushAllPages()
		hf.bufPool.FlushAllPages()
	}

	return nil
}

// Function to make column oriented transitdb database
func MakeColumnOrientedTransitDatabase() error {
	gated_station_entries := TupleDesc{Fields: []FieldType{
		{Fname: "service_date", Ftype: StringType},
		{Fname: "time", Ftype: StringType},
		{Fname: "station_id", Ftype: StringType},
		{Fname: "line_id", Ftype: StringType},
		{Fname: "gated_entries", Ftype: IntType},
	}}
	// lines := TupleDesc{Fields: []FieldType{
	// 	{Fname: "line_id", Ftype: StringType},
	// 	{Fname: "line_name", Ftype: StringType},
	// }}
	// routes := TupleDesc{Fields: []FieldType{
	// 	{Fname: "route_id", Ftype: IntType},
	// 	{Fname: "line_id", Ftype: StringType},
	// 	{Fname: "first_station_id", Ftype: StringType},
	// 	{Fname: "last_station_id", Ftype: StringType},
	// 	{Fname: "direction", Ftype: IntType},
	// 	{Fname: "direction_desc", Ftype: StringType},
	// 	{Fname: "route_name", Ftype: StringType}}}
	// stations := TupleDesc{Fields: []FieldType{
	// 	{Fname: "station_id", Ftype: StringType},
	// 	{Fname: "station_name", Ftype: StringType},
	// }}
	// rail_ridership := TupleDesc{Fields: []FieldType{
	// 	{Fname: "season", Ftype: StringType},
	// 	{Fname: "line_id", Ftype: StringType}, {Fname: "direction", Ftype: IntType},
	// 	{Fname: "time_period_id", Ftype: StringType},
	// 	{Fname: "station_id", Ftype: StringType},
	// 	{Fname: "total_ons", Ftype: IntType},
	// 	{Fname: "total_offs", Ftype: IntType},
	// 	{Fname: "number_service_days", Ftype: IntType},
	// 	{Fname: "average_ons", Ftype: IntType},
	// 	{Fname: "average_offs", Ftype: IntType},
	// 	{Fname: "average_flow", Ftype: IntType},
	// }}
	// station_orders := TupleDesc{Fields: []FieldType{
	// 	{Fname: "route_id", Ftype: IntType},
	// 	{Fname: "station_id", Ftype: StringType},
	// 	{Fname: "stop_order", Ftype: IntType},
	// 	{Fname: "distance_from_last_station_miles", Ftype: IntType},
	// }}
	// time_periods := TupleDesc{Fields: []FieldType{
	// 	{Fname: "time_period_id", Ftype: StringType},
	// 	{Fname: "day_type", Ftype: StringType},
	// 	{Fname: "time_period", Ftype: StringType},
	// 	{Fname: "period_start_time", Ftype: StringType},
	// 	{Fname: "period_end_time", Ftype: StringType},
	// }}
	bp1 := NewBufferPool(20)
	bp2 := NewBufferPool(20)
	hf, err := NewHeapFile("godb/transitdb/gated_station_entries.dat", &gated_station_entries, bp1)
	cf, err := NewColumnFile("godb/transitdb/gated_station_entries.dat", &gated_station_entries, bp2)
	if err != nil {
		return err
	}
	tid := NewTID()
	hf_iter, _ := hf.Iterator(tid, &gated_station_entries)
	for {
		t, _ := hf_iter()
		if t == nil {
			cf.bufPool.FlushAllPages()
			return nil
		}
		cf.bufPool.FlushAllPages()
		cf.insertTuple(t, tid)
	}
}

// ColumnFile represents a file that stores a specific column
// Each column file has a descriptor desc of all the columns it contains
// Each column file has an array of all the heapfiles that represent a column in the table
type ColumnFile struct {
	name           string
	Desc           *TupleDesc
	ColumnFiles    []*HeapFile
	bufPool        *BufferPool
	ColumnFilesMap map[string]*HeapFile
}

// Function to make a new column file
func NewColumnFile(name string, td *TupleDesc, bp *BufferPool) (*ColumnFile, error) {
	// make a new column file from
	heapFiles := []*HeapFile{}
	heapFilesMap := map[string]*HeapFile{}
	for i := range td.Fields {
		columnTd := &TupleDesc{Fields: []FieldType{td.Fields[i]}}
		fromFile := name + "_" + td.Fields[i].Fname + ".dat"
		columnHeapFile, _ := NewHeapFile(fromFile, columnTd, bp)
		heapFiles = append(heapFiles, columnHeapFile)
		heapFilesMap[td.Fields[i].Fname] = columnHeapFile
	}
	return &ColumnFile{
		name:           name,
		Desc:           td,
		ColumnFiles:    heapFiles,
		bufPool:        bp,
		ColumnFilesMap: heapFilesMap,
	}, nil
}

// insertTuple
func (cf *ColumnFile) insertTuple(t *Tuple, tid TransactionID) error {
	if len(t.Fields) != len(cf.ColumnFiles) {
		return GoDBError{code: IllegalOperationError, errString: "Could not insert Tuple"}
	}
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
	if len(t.Fields) != len(cf.ColumnFiles) {
		return GoDBError{code: IllegalOperationError, errString: "Could not delete Tuple"}
	}
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
func (cf *ColumnFile) Iterator(tid TransactionID, selectDesc *TupleDesc) (func() (*Tuple, error), error) {
	iterators := []func() (*Tuple, error){}
	for _, field := range selectDesc.Fields {
		f := cf.ColumnFilesMap[field.Fname]
		if f == nil {
			it, _ := cf.ColumnFiles[0].Iterator(tid, cf.Descriptor())
			iterators = append(iterators, it)
		} else {
			it, _ := f.Iterator(tid, f.Descriptor())
			iterators = append(iterators, it)
		}
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

// placehodler function to implement DBFile
func (cf *ColumnFile) flushPage(p *Page) error {
	return nil
}

// Placeholder function to implement DBFile
func (f *ColumnFile) pageKey(pgNo int) any {
	return nil
}

// Placeholder function to implement DBFile
func (f *ColumnFile) readPage(pageNo int) (*Page, error) {
	return nil, nil
}
