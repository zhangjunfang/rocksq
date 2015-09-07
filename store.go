package rocksq

import (
	"os"
	"runtime"
	"sync"

	"github.com/mijia/sweb/log"
	rocks "github.com/tecbot/gorocksdb"
)

type StoreOptions struct {
	Directory  string
	MemorySize int
	IsDebug    bool
}

type Store struct {
	*rocks.DB
	sync.RWMutex

	directory string
	dbOpts    *rocks.Options

	cfHandles map[string]*rocks.ColumnFamilyHandle
	queues    map[string]*Queue
	ro        *rocks.ReadOptions
	wo        *rocks.WriteOptions
}

func (s *Store) NewQueue(name string) (*Queue, error) {
	s.Lock()
	defer s.Unlock()

	if q, ok := s.queues[name]; ok {
		return q, nil
	}
	var cfHandle *rocks.ColumnFamilyHandle
	if handle, ok := s.cfHandles[name]; ok {
		cfHandle = handle
	} else {
		if handle, err := s.CreateColumnFamily(s.dbOpts, name); err != nil {
			log.Errorf("Failed to create column family %q, %s", name, err)
			return nil, err
		} else {
			cfHandle = handle
			s.cfHandles[name] = cfHandle
		}
	}
	return newQueue(name, s, cfHandle), nil
}

func (s *Store) Close() {
	for _, handle := range s.cfHandles {
		handle.Destroy()
	}
	s.DB.Close()
	s.dbOpts.Destroy()
	s.ro.Destroy()
	s.wo.Destroy()
}

func (s *Store) Destroy() {
	s.Close()
	rocks.DestroyDb(s.directory, rocks.NewDefaultOptions())
}

func NewStore(options StoreOptions) (*Store, error) {
	if options.IsDebug {
		log.EnableDebug()
	}

	s := &Store{
		directory: options.Directory,
		cfHandles: make(map[string]*rocks.ColumnFamilyHandle),
		queues:    make(map[string]*Queue),
	}

	opts := rocks.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.IncreaseParallelism(runtime.NumCPU())

	opts.SetWriteBufferSize(64 * 1024 * 1024)
	opts.SetMaxWriteBufferNumber(3)
	opts.SetTargetFileSizeBase(64 * 1024 * 1024)
	opts.SetLevel0FileNumCompactionTrigger(8)
	opts.SetLevel0SlowdownWritesTrigger(17)
	opts.SetLevel0StopWritesTrigger(24)
	opts.SetNumLevels(4)
	opts.SetMaxBytesForLevelBase(512 * 1024 * 1024)
	opts.SetMaxBytesForLevelMultiplier(8)
	opts.SetCompression(0)

	bbto := rocks.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(rocks.NewLRUCache(options.MemorySize))
	bbto.SetFilterPolicy(rocks.NewBloomFilter(10))
	opts.SetBlockBasedTableFactory(bbto)

	opts.SetMaxOpenFiles(-1)
	opts.SetMemtablePrefixBloomBits(8 * 1024 * 1024)

	var err error
	if err = os.MkdirAll(options.Directory, 0755); err != nil {
		log.Errorf("Failed to mkdir %q, %s", options.Directory, err)
		return nil, err
	}

	cfNames, err := rocks.ListColumnFamilies(opts, options.Directory)
	if err != nil {
		// FIXME: we need to be sure if this means the db does not exist for now
		// so that we cannot list the column families
		log.Errorf("Failed to collect the column family names, %s", err)
	} else {
		log.Debugf("Got column family names for the existing db, %+v", cfNames)
	}

	if len(cfNames) == 0 {
		// We create the default column family to get the column family handle
		cfNames = []string{"default"}
	}
	cfOpts := make([]*rocks.Options, len(cfNames))
	for i := range cfNames {
		cfOpts[i] = opts
	}
	db, cfHandles, err := rocks.OpenDbColumnFamilies(opts, options.Directory, cfNames, cfOpts)
	if err != nil {
		log.Errorf("Failed to open rocks database, %s", err)
		return nil, err
	}

	s.DB = db
	s.dbOpts = opts
	s.ro = rocks.NewDefaultReadOptions()
	s.wo = rocks.NewDefaultWriteOptions()

	if len(cfNames) > 0 {
		for i := range cfNames {
			s.cfHandles[cfNames[i]] = cfHandles[i]
		}
	}
	return s, nil
}
