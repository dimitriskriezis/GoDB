package godb

import (
	"sync"
	"time"

	"golang.org/x/exp/slices"
)

// import (
// 	godb "command-line-argumentsC:\\Users\\dimit\\Documents\\6.5381\\go-db-hw-2023\\godb\\buffer_pool.go"
// 	godb "command-line-argumentsC:\\Users\\dimit\\Documents\\6.5381\\go-db-hw-2023\\godb\\types.go"
// )

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type LockWait struct {
	tid  TransactionID
	page any
}

type BufferPool struct {
	Size           int
	Pages          map[any]*Page
	Order          []any
	Mutex          sync.Mutex
	SharedLocks    map[any][]TransactionID      // map that keeps track of which transactions have a lock on a specific page
	ExclusiveLocks map[any]TransactionID        // map that keeps track of which page transaction has an exclusive lock on a specific pageId
	waitGraph      map[TransactionID][]LockWait // map that keeps track of which transaction waits on what other transactions
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	return &BufferPool{
		Size:           numPages,
		Pages:          map[any]*Page{},
		Order:          []any{},
		SharedLocks:    map[any][]TransactionID{},
		ExclusiveLocks: map[any]TransactionID{},
		waitGraph:      map[TransactionID][]LockWait{},
	}
}

func (bp *BufferPool) addLockWait(waitingTid TransactionID, waitingOnTid TransactionID, lockedPage any) {
	edge := LockWait{waitingOnTid, lockedPage}
	// if edge already included
	for _, currentEdge := range bp.waitGraph[waitingTid] {
		if currentEdge == edge {
			return
		}
	}
	// add edge in graph
	bp.waitGraph[waitingTid] = append(bp.waitGraph[waitingTid], edge)
}

// Remove lock from graph
func (bp *BufferPool) removeLockWait(tid TransactionID, pageId any) {
	// Before I acquire a lock, check if I was waiting for that lock to remove it from the wait graph
	index := -1
	for i, val := range bp.waitGraph[tid] {
		// If I was waiting for a lock on that
		if val.page == pageId {
			index = i
		}
	}
	// if I was waiting for a lock remove it from the graph
	if index > 0 {
		bp.waitGraph[tid] = append(bp.waitGraph[tid][:index], bp.waitGraph[tid][index+1:]...)
	}
}

func (bp *BufferPool) removeTransactionFromWaitGraph(tid TransactionID) {
	// If I commit or abort remove transaction node as well as all edges into transaction node from the graph
	// if transaction in wait graph remove the transaction
	if _, ok := bp.waitGraph[tid]; ok {
		// delete all outgoing edges
		delete(bp.waitGraph, tid)
		// delete all incoming edges
		for key := range bp.waitGraph {
			for _, edge := range bp.waitGraph[key] {
				if edge.tid == tid {
					bp.removeLockWait(key, edge.page)
				}
			}
		}
	}
}

func dfs(bp *BufferPool, startnode TransactionID, visited *map[TransactionID]bool) bool {
	(*visited)[startnode] = true
	graph := bp.waitGraph
	for _, child := range graph[startnode] {
		// if child is visited in the recursion stack there is a cycle
		if _, ok := (*visited)[child.tid]; ok {
			return true
		}
		val := dfs(bp, child.tid, visited) // explore all other tids
		if val {
			return val
		}
	}
	return false
}

func (bp *BufferPool) detectCycle(tid TransactionID) bool {
	visited := map[TransactionID]bool{}
	return dfs(bp, tid, &visited)
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	for pageKey, pagePtr := range bp.Pages {
		page := *pagePtr
		dbfile := *page.getFile()
		dbfile.flushPage(&page)
		delete(bp.Pages, pageKey)
	}
	bp.Order = []any{}
}

func IndexOf(array []any, val any) int {
	for i := range array {
		if array[i] == val {
			return i
		}
	}
	return -1
}

func TransactionIndexOf(array []TransactionID, val any) int {
	for i := range array {
		if array[i] == val {
			return i
		}
	}
	return -1
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	println("I am aborting")
	bp.Mutex.Lock()
	bp.removeTransactionFromWaitGraph(tid)
	// release all read locks by tid
	for pageId := range bp.SharedLocks {
		if slices.Contains(bp.SharedLocks[pageId], tid) {
			index := TransactionIndexOf(bp.SharedLocks[pageId], tid)
			bp.SharedLocks[pageId] = append(bp.SharedLocks[pageId][:index], bp.SharedLocks[pageId][index+1:]...)
		}
		if len(bp.SharedLocks[pageId]) == 0 {
			delete(bp.SharedLocks, pageId)
		}
	}
	println(bp.Pages)
	for pageId, pageTid := range bp.ExclusiveLocks {
		// If the file is locked by this transaction delete it
		if pageTid == tid {
			_, ok := bp.Pages[pageId]
			if ok {
				// if pageId exists in buferpool delete page and update order
				delete(bp.Pages, pageId)
				index := IndexOf(bp.Order, pageId)
				bp.Order = append(bp.Order[:index], bp.Order[index+1:]...)
			}
			delete(bp.ExclusiveLocks, pageId)
		}
	}
	println(bp.Pages)
	bp.Mutex.Unlock()
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	bp.Mutex.Lock()
	// println("I am committing ", tid)
	// for key, value := range bp.ExclusiveLocks {
	// 	println(key, value)
	// }
	bp.removeTransactionFromWaitGraph(tid)
	// flush each page tid edited to disk
	for pageId, pageTid := range bp.ExclusiveLocks {
		// If the file is locked by this transaction flush it
		if pageTid == tid {
			toEvictPage, ok := bp.Pages[pageId]
			if ok {
				dbfile := *(*toEvictPage).getFile()
				dbfile.flushPage(toEvictPage)
				delete(bp.Pages, pageId)
				index := IndexOf(bp.Order, pageId)
				bp.Order = append(bp.Order[:index], bp.Order[index+1:]...)
			}
			delete(bp.ExclusiveLocks, pageId)
		}
	}
	// release all read locks by this tid
	for pageId := range bp.SharedLocks {
		if slices.Contains(bp.SharedLocks[pageId], tid) {
			index := TransactionIndexOf(bp.SharedLocks[pageId], tid)
			bp.SharedLocks[pageId] = append(bp.SharedLocks[pageId][:index], bp.SharedLocks[pageId][index+1:]...)
		}
		if len(bp.SharedLocks[pageId]) == 0 {
			delete(bp.SharedLocks, pageId)
		}
	}
	bp.Mutex.Unlock()
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	// println("Info", tid, pageNo, perm)
	pageKey := file.pageKey(pageNo)
	for {
		bp.Mutex.Lock()
		// if write perm check if there is any lock on this page
		if perm == WritePerm {
			_, areSharedLocks := bp.SharedLocks[pageKey]
			exclusiveLockTid, isExclusiveLock := bp.ExclusiveLocks[pageKey]
			// if there are no locks on the page get exclusive lock
			// if there are no exclusive locks check shared locks
			//
			if isExclusiveLock {
				// if there is exclusive locks on the page if another tid holds the lock sleep else if you hold it break
				if exclusiveLockTid != tid {
					// If lock is being held try to add edge to the wait graph
					bp.addLockWait(tid, exclusiveLockTid, pageKey)
					// if cycle abort
					if bp.detectCycle(tid) {
						// release mutex
						println(tid)
						bp.Mutex.Unlock()
						bp.AbortTransaction(tid)
						// throw error
						return nil, GoDBError{code: DeadlockError, errString: "Transaction deadlocked"}
					}
					// println("Current Locks")
					// for key, value := range bp.ExclusiveLocks {
					// 	println(key, value)
					// }
					// println("I am waiting for: ", pageKey, exclusiveLockTid)
					// for key, value := range bp.waitGraph {
					// 	print(key, ": ")
					// 	for _, val := range value {
					// 		print(val.tid, " ")
					// 	}
					// 	println()
					// }
					println("here")
					bp.Mutex.Unlock()
					time.Sleep(10 * time.Microsecond)
				} else {
					break
				}

			} else if areSharedLocks {
				// if tid has a shared lock and it is the only shared lock upgrade to exclusive else sleep
				if slices.Contains(bp.SharedLocks[pageKey], tid) && len(bp.SharedLocks[pageKey]) == 1 {
					bp.SharedLocks[pageKey] = []TransactionID{}
					bp.ExclusiveLocks[pageKey] = tid
					break
				} else {
					// add an edge waiting for every shared in
					for _, sharedTid := range bp.SharedLocks[pageKey] {
						if tid != sharedTid {
							bp.addLockWait(tid, sharedTid, pageKey)
						}
					}
					// if cycle abort
					if bp.detectCycle(tid) {
						println(tid)
						// release mutex
						bp.Mutex.Unlock()
						// abort transaction
						bp.AbortTransaction(tid)
						// throw error
						return nil, GoDBError{code: DeadlockError, errString: "Transaction deadlocked"}
					}
					bp.Mutex.Unlock()
					// print("here1")
					time.Sleep(10 * time.Microsecond)
				}
			} else {
				// no locks just acquire the write lock
				bp.ExclusiveLocks[pageKey] = tid
				break
			}
		}

		// if read perm check if there is any exclusive lock on this page
		if perm == ReadPerm {
			_, areSharedLocks := bp.SharedLocks[pageKey]
			exclusiveLockTid, isExclusiveLock := bp.ExclusiveLocks[pageKey]
			// if there is exclusive lock by some other transaction sleep
			// else if there are shared locks by other transaction but not by this tid obtain a read transaction
			// otherwise if there are no shared locks on this page acquire a shared lock on the page
			// if successfully acquire lock break from the for loop but don't unlock. Get page and unlock before return
			if isExclusiveLock {
				// if I don't hold the lock sleep else break
				if exclusiveLockTid != tid {
					bp.addLockWait(tid, exclusiveLockTid, pageKey)
					// if cycle abort
					if bp.detectCycle(tid) {
						// release mutex
						bp.Mutex.Unlock()
						// abort transaction
						bp.AbortTransaction(tid)
						// throw error
						return nil, GoDBError{code: DeadlockError, errString: "Transaction deadlocked"}
					}
					print("here3")
					bp.Mutex.Unlock()
					time.Sleep(10 * time.Microsecond)
				} else {
					break
				}
			} else if areSharedLocks {
				// if there is no shared lock by this transaction acquire it and then just read
				if !slices.Contains(bp.SharedLocks[pageKey], tid) {
					bp.SharedLocks[pageKey] = append(bp.SharedLocks[pageKey], tid)
				}
				break
			} else {
				// if there are no locks just take one by this tid
				bp.SharedLocks[pageKey] = []TransactionID{tid}
				break
			}
		}
	}
	bpPage, ok := bp.Pages[pageKey]
	// If page in buffer pool retrieve page from the buffer pool
	if ok {
		bp.Mutex.Unlock()
		return bpPage, nil
	}
	// If page not in buffer pool no one has a lock on it so we are first move page to memory take lock
	diskPage, diskReadError := file.readPage(pageNo)
	if diskReadError != nil {
		bp.Mutex.Unlock()
		return nil, diskReadError
	}
	// If buffer pool has space add diskPage to bp
	if len(bp.Pages) < bp.Size {
		bp.Pages[pageKey] = diskPage
		bp.Order = append(bp.Order, pageKey)
		bp.Mutex.Unlock()
		return diskPage, nil
	}
	// Buffer pool doesn't have space. Get LRU clean page id and evict it. If none throw error
	for i := 0; i < len(bp.Order); i++ {
		currentPage := *bp.Pages[bp.Order[i]]
		if !currentPage.isDirty() {
			// Remove LRU
			delete(bp.Pages, bp.Order[i])
			bp.Order = append(bp.Order[:i], bp.Order[i+1:]...)
			// Add current page
			bp.Pages[pageKey] = diskPage
			bp.Order = append(bp.Order, pageKey)
			bp.Mutex.Unlock()
			return diskPage, nil
		}
	}

	// Buffer pool has only dirty entries
	bp.Mutex.Unlock()
	return nil, GoDBError{code: BufferPoolFullError, errString: "Buffer is full of dirty pages"}
}
