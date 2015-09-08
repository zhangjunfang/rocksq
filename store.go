package rocksq

import (
	"os"
	"runtime"
	"sync"

	"github.com/mijia/sweb/log"
	rocks "github.com/tecbot/gorocksdb"
)

// StoreOptions defines the options for rocksdb storage
type StoreOptions struct {
	Directory             string
	WriteBufferSize       int
	WriteBufferNumber     int
	MemorySize            int
	FileSizeBase          uint64
	Compression           rocks.CompressionType
	Parallel              int
	DisableAutoCompaction bool
	DisableWAL            bool
	DisableTailing        bool
	Sync                  bool
	IsDebug               bool
}

func (so *StoreOptions) SetDefaults() {
	if so.MemorySize <= 0 {
		so.MemorySize = 8 * 1024 * 1024
	}
	if so.FileSizeBase <= 0 {
		so.FileSizeBase = 64 * 1024 * 1024
	}
	if so.WriteBufferSize <= 0 {
		so.WriteBufferSize = 64 * 1024 * 1024
	}
	if so.WriteBufferNumber <= 0 {
		so.WriteBufferNumber = 4
	}
	if so.Parallel <= 0 {
		so.Parallel = runtime.NumCPU()
	}
}

// Store defines the basic rocksdb wrapper
type Store struct {
	*rocks.DB
	sync.RWMutex

	directory  string
	useTailing bool
	dbOpts     *rocks.Options

	cfHandles map[string]*rocks.ColumnFamilyHandle
	queues    map[string]*Queue
	ro        *rocks.ReadOptions
	wo        *rocks.WriteOptions
}

// NewQueue will return a named queue using Column Family from RocksDB
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
	return newQueue(name, s, cfHandle, s.useTailing), nil
}

// Close the rocksdb database
func (s *Store) Close() {
	for _, queue := range s.queues {
		queue.Close()
	}
	for _, handle := range s.cfHandles {
		handle.Destroy()
	}
	s.DB.Close()
}

// Destroy the rocksdb instance and data files
func (s *Store) Destroy() {
	s.Close()
	rocks.DestroyDb(s.directory, rocks.NewDefaultOptions())
}

// NewStore returns the Store a rocksdb wrapper
func NewStore(options StoreOptions) (*Store, error) {
	options.SetDefaults()
	if options.IsDebug {
		log.EnableDebug()
	}

	s := &Store{
		directory:  options.Directory,
		useTailing: !options.DisableTailing,
		cfHandles:  make(map[string]*rocks.ColumnFamilyHandle),
		queues:     make(map[string]*Queue),
	}

	opts := rocks.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.IncreaseParallelism(options.Parallel)
	opts.SetMergeOperator(&_CountMerger{})
	opts.SetMaxSuccessiveMerges(64)

	opts.SetWriteBufferSize(options.WriteBufferSize)
	opts.SetMaxWriteBufferNumber(options.WriteBufferNumber)
	opts.SetTargetFileSizeBase(options.FileSizeBase)
	opts.SetLevel0FileNumCompactionTrigger(8)
	opts.SetLevel0SlowdownWritesTrigger(16)
	opts.SetLevel0StopWritesTrigger(24)
	opts.SetNumLevels(4)
	opts.SetMaxBytesForLevelBase(512 * 1024 * 1024)
	opts.SetMaxBytesForLevelMultiplier(8)
	opts.SetCompression(options.Compression)
	opts.SetDisableAutoCompactions(options.DisableAutoCompaction)

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
	s.ro.SetFillCache(false)
	s.ro.SetTailing(!options.DisableTailing)
	s.wo = rocks.NewDefaultWriteOptions()
	s.wo.DisableWAL(options.DisableWAL)
	s.wo.SetSync(options.Sync)

	if len(cfNames) > 0 {
		for i := range cfNames {
			s.cfHandles[cfNames[i]] = cfHandles[i]
		}
	}
	return s, nil
}
