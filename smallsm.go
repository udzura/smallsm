package smallsm

import (
	"os"
	"sync"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

type SmalLSM struct {
	mu sync.RWMutex

	memTable        *SmalMemTable
	memtableWorking *SmalMemTable
	sstables        []*SmalSSTable

	logdir string
	// TODO: compaction
	// inCompaction bool
}

type SmalMemTable struct {
	tree rbt.Tree
}

type SmalLog struct {
	Key, Value string
	Deleted    bool
}

type SmalSSTable struct {
	prefix              string
	index               map[string]int64
	indexLoadedOnMemory bool

	file               *os.File
	dataLoadedOnMemory bool
}

func New(dir string) *SmalLSM {
	os.MkdirAll(dir, 0700)
	memTable := &SmalMemTable{
		tree: *rbt.NewWithStringComparator(),
	}

	return &SmalLSM{
		memTable:        memTable,
		memtableWorking: nil,
		sstables:        make([]*SmalSSTable, 0),
		logdir:          dir,
	}
}

func (lsm *SmalLSM) Get(key string) (string, bool) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
	data, ok := lsm.memTable.tree.Get(key)
	if !ok {
		return "", false
	}
	log := data.(*SmalLog)
	if log.Deleted {
		return "", false
	}

	return log.Value, true
}

func (lsm *SmalLSM) Put(key, value string) {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	log := &SmalLog{
		Key:   key,
		Value: value,
	}
	lsm.memTable.tree.Put(key, log)
}

func (lsm *SmalLSM) Delete(key string) {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	log := &SmalLog{
		Key:     key,
		Value:   "",
		Deleted: true,
	}
	lsm.memTable.tree.Put(key, log)
}
