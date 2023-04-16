package smallsm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

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
	Key     string `json:"k"`
	Value   string `json:"v"`
	Deleted bool   `json:"d"`
}

type SmalSSTable struct {
	prefix string
	index  map[string]int64
	file   *os.File
}

func New(dir string) (*SmalLSM, error) {
	os.MkdirAll(dir, 0o0700)
	memTable := &SmalMemTable{
		tree: *rbt.NewWithStringComparator(),
	}

	sstables := make([]*SmalSSTable, 0)
	logs, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return nil, err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(logs)))
	for _, path := range logs {
		prefix := filepath.Base(path)
		prefix = prefix[0 : len(prefix)-4] // chop `.log`
		sst, err := LoadSSTable(dir, prefix)
		if err != nil {
			return nil, err
		}
		sstables = append(sstables, sst)
	}

	return &SmalLSM{
		memTable:        memTable,
		memtableWorking: nil,
		sstables:        sstables,
		logdir:          dir,
	}, nil
}

func LoadSSTable(dir, prefix string) (*SmalSSTable, error) {
	sst := &SmalSSTable{
		prefix: prefix,
		index:  make(map[string]int64),
	}

	indexf, err := os.Open(filepath.Join(dir, prefix+".index"))
	if err != nil {
		return nil, err
	}
	defer indexf.Close()
	if err := json.NewDecoder(indexf).Decode(&sst.index); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(dir, prefix+".log"))
	sst.file = f

	return sst, nil
}

func (lsm *SmalLSM) Get(key string) (string, bool) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
	var log *SmalLog = nil
	data, ok := lsm.memTable.tree.Get(key)
	if !ok {
		if lsm.memtableWorking != nil {
			data, ok := lsm.memtableWorking.tree.Get(key)
			if ok {
				log = data.(*SmalLog)
			}
		}

		if log == nil {
			for _, sst := range lsm.sstables {
				data, ok := sst.Get(key)
				if ok {
					log = data
					break
				}
			}
		}
	} else {
		log = data.(*SmalLog)
	}
	if log == nil {
		return "", false
	}

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
	log := &SmalLog{
		Key:     key,
		Value:   "",
		Deleted: true,
	}
	lsm.memTable.tree.Put(key, log)
}

func (lsm *SmalLSM) MigrateToSSTable() error {
	lsm.mu.Lock()
	lsm.memtableWorking = lsm.memTable
	lsm.memTable = &SmalMemTable{
		tree: *rbt.NewWithStringComparator(),
	}
	lsm.mu.Unlock()

	prefix := fmt.Sprintf("%024d", time.Now().UnixNano())
	sst := &SmalSSTable{
		prefix: prefix,
		index:  make(map[string]int64),
	}
	logf, err := os.OpenFile(
		filepath.Join(lsm.logdir, prefix+".log"),
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0o0600,
	)
	if err != nil {
		return err
	}
	defer logf.Close()
	logWriter := json.NewEncoder(logf)
	indexf, err := os.OpenFile(
		filepath.Join(lsm.logdir, prefix+".index"),
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0o0600,
	)
	if err != nil {
		return err
	}
	defer indexf.Close()

	for _, key := range lsm.memtableWorking.tree.Keys() {
		key := key.(string)
		data, ok := lsm.memtableWorking.tree.Get(key)
		if !ok {
			continue
		}
		log := data.(*SmalLog)
		offset, err := logf.Seek(0, 1)
		if err != nil {
			return err
		}
		if err := logWriter.Encode(log); err != nil {
			return err
		}

		sst.index[key] = offset
	}

	if err := json.NewEncoder(indexf).Encode(&sst.index); err != nil {
		return err
	}
	_ = logf.Close()

	// reopen
	f, err := os.Open(filepath.Join(lsm.logdir, prefix+".log"))
	if err != nil {
		return err
	}

	sst.file = f

	lsm.mu.Lock()
	lsm.sstables = append([]*SmalSSTable{sst}, lsm.sstables...)
	lsm.memtableWorking = nil
	lsm.mu.Unlock()

	return nil
}

func (sst *SmalSSTable) Get(key string) (*SmalLog, bool) {
	off, ok := sst.index[key]
	if !ok {
		return nil, false
	}
	_, err := sst.file.Seek(off, 0)
	if err != nil {
		// TODO when cannnot open
		panic(err)
	}
	defer sst.file.Seek(0, 0)

	log := &SmalLog{}
	if err := json.NewDecoder(sst.file).Decode(log); err != nil {
		panic(err)
	}

	return log, true
}
