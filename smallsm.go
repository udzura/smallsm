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

	// TODO: compaction
	// inCompaction bool
}

type SmalMemTable struct {
	tree rbt.Tree
}

type SmalSSTable struct {
	prefix              string
	index               map[string]int64
	indexLoadedOnMemory bool

	file               *os.File
	dataLoadedOnMemory bool
}
