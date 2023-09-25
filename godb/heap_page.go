package godb

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	Hfile     *HeapFile
	Dirty     bool
	pageNo    int
	Desc      *TupleDesc
	Slots     map[int]*Tuple
	UsedSlots []bool
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	heap := &heapPage{
		Hfile:  f,
		Dirty:  false,
		pageNo: pageNo,
		Desc:   desc,
		Slots:  map[int]*Tuple{},
	} //replace me
	numSlots := heap.getNumSlots()
	heap.UsedSlots = make([]bool, numSlots)
	return heap
}

func (h *heapPage) getNumSlots() int {
	slotSize := 0
	for i := 0; i < len(h.Desc.Fields); i++ {
		fieldSize := 0
		if h.Desc.Fields[i].Ftype == StringType { // is of type StringField
			fieldSize += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
		} else { // is of type int
			fieldSize += (int)(unsafe.Sizeof(int64(0)))
		}
		slotSize += fieldSize
	}
	return slotSize
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	for i := range h.UsedSlots {
		if !h.UsedSlots[i] {
			h.UsedSlots[i] = true
			rid := RecordID{pageNo: h.pageNo, slot: i}
			t.Rid = rid
			h.Slots[i] = t
			return rid, nil
		}
	}
	return nil, GoDBError{code: PageFullError, errString: "Page slots are full. Cannot add tuple to page"}
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	for slot := range h.Slots {
		if slot == rid.slot {
			delete(h.Slots, slot)
			h.UsedSlots[slot] = false
			return nil
		}
	}
	return GoDBError{code: TupleNotFoundError, errString: "Could not delete tuple with recordId rid"}
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.Dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	h.Dirty = true
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	var file DBFile = p.Hfile
	return &file
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// initialize buffer
	b := new(bytes.Buffer)
	// write number of slots to buffer
	numberOfSlots := h.getNumSlots()
	nos_error := binary.Write(b, binary.LittleEndian, numberOfSlots)
	if nos_error != nil {
		return nil, nos_error
	}
	// write number of used slots to buffer
	numberOfUsedSlots := len(h.Slots)
	nous_error := binary.Write(b, binary.LittleEndian, numberOfUsedSlots)
	if nous_error != nil {
		return nil, nous_error
	}
	for _, tuple := range h.Slots {
		tuple_error := tuple.writeTo(b)
		if tuple_error != nil {
			return nil, tuple_error
		}
	}

	return nil, nil //replace me

}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	var numberOfSlots int32
	var numberOfUsedSlots int32
	binary.Read(buf, binary.LittleEndian, numberOfSlots)
	binary.Read(buf, binary.LittleEndian, numberOfUsedSlots)
	for buf.Len() > 0 {
		tuple, err := readTupleFrom(buf, h.Desc)
		if err != nil {
			return err
		}
		h.insertTuple(tuple)
	}
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	i := 0
	return func() (*Tuple, error) {
		for i < p.getNumSlots() {
			t, ok := p.Slots[i]
			// If found slot return tuple
			if ok {
				i += 1
				return t, nil
			}
			// Otherwise increment i until you get a slot
			i += 1
		}
		// If i is larger than num Slots then we are done iterating
		return nil, nil
	}
}
