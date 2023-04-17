package smallsm

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/udzura/smallsm/codec"
	slog "github.com/udzura/smallsm/log"
)

var (
	indexInterval int = 10 // Changeme
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
	tree *rbt.Tree
}

type SmalSSTable struct {
	prefix string
	index  *rbt.Tree
	file   *os.File
}

func New(dir string) (*SmalLSM, error) {
	os.MkdirAll(dir, 0o0700)
	memTable := &SmalMemTable{
		tree: rbt.NewWithStringComparator(),
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
		index:  rbt.NewWithStringComparator(),
	}

	indexf, err := os.Open(filepath.Join(dir, prefix+".index"))
	if err != nil {
		return nil, err
	}
	defer indexf.Close()
	data, err := io.ReadAll(indexf)
	if err != nil {
		return nil, err
	}
	if err := sst.index.FromJSON(data); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(dir, prefix+".log"))
	sst.file = f

	return sst, nil
}

func (lsm *SmalLSM) Get(key string) (string, bool) {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()
	var log *slog.Log = nil
	data, ok := lsm.memTable.tree.Get(key)
	if !ok {
		if lsm.memtableWorking != nil {
			data, ok := lsm.memtableWorking.tree.Get(key)
			if ok {
				log = data.(*slog.Log)
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
		log = data.(*slog.Log)
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
	log := &slog.Log{
		Key:   key,
		Value: value,
	}
	lsm.memTable.tree.Put(key, log)
}

func (lsm *SmalLSM) Delete(key string) {
	lsm.mu.Lock()
	defer lsm.mu.Unlock()
	log := &slog.Log{
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
		tree: rbt.NewWithStringComparator(),
	}
	lsm.mu.Unlock()

	prefix := fmt.Sprintf("%024d", time.Now().UnixNano())
	sst := &SmalSSTable{
		prefix: prefix,
		index:  rbt.NewWithStringComparator(),
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
	logWriter := codec.NewEncoder(logf)
	indexf, err := os.OpenFile(
		filepath.Join(lsm.logdir, prefix+".index"),
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0o0600,
	)
	if err != nil {
		return err
	}
	defer indexf.Close()

	for i, key := range lsm.memtableWorking.tree.Keys() {
		key := key.(string)
		data, ok := lsm.memtableWorking.tree.Get(key)
		if !ok {
			continue
		}
		log := data.(*slog.Log)
		offset, err := logf.Seek(0, 1)
		if err != nil {
			return err
		}
		if err := logWriter.Encode(log); err != nil {
			return err
		}

		if i%indexInterval == 0 {
			sst.index.Put(key, offset)
		}
	}

	indexData, err := sst.index.ToJSON()
	if err != nil {
	}
	if _, err := indexf.Write(indexData); err != nil {
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

func (sst *SmalSSTable) Get(key string) (*slog.Log, bool) {
	prevkey := ""
	for _, currentkey := range sst.index.Keys() {
		if key < currentkey.(string) {
			break
		}
		prevkey = currentkey.(string)
	}

	if prevkey == "" {
		return nil, false
	}

	off_, ok := sst.index.Get(prevkey)
	if !ok {
		return nil, false
	}
	var off int64
	switch v := off_.(type) {
	case int64:
		off = v
	case float64:
		off = int64(v)
	}

	_, err := sst.file.Seek(off, 0)
	if err != nil {
		// TODO when cannnot open
		panic(err)
	}
	defer sst.file.Seek(0, 0)

	for i := 0; i < indexInterval; i++ {
		log, err := codec.NewDecoder(sst.file).Decode()
		if err != nil {
			panic(err)
		}
		if log.Key == key {
			return log, true
		}
	}

	return nil, false
}
