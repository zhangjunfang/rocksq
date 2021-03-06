package rocksq

import (
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"sync/atomic"

	"github.com/mijia/sweb/log"
	"github.com/oxtoacart/bpool"
	rocks "github.com/tecbot/gorocksdb"
)

var (
	EmptyQueue = errors.New("No new message in the queue")
	oneBinary  []byte
)

type Queue struct {
	name       string
	head       uint64
	tail       uint64
	useTailing bool

	cfHandle     *rocks.ColumnFamilyHandle
	tailIterator *rocks.Iterator
	store        *Store
	bufPool      *bpool.BufferPool
}

func (q *Queue) Close() {
	if q.useTailing {
		q.tailIterator.Close()
	}
}

func (q *Queue) ApproximateSize() uint64 {
	head := q.getIndexId("head", 1)
	tail := q.getIndexId("tail", 0)
	return tail - head + 1
}

func (q *Queue) Enqueue(data []byte) (uint64, error) {
	id := atomic.AddUint64(&q.tail, 1)
	wb := rocks.NewWriteBatch()
	defer wb.Destroy()
	wb.MergeCF(q.cfHandle, q.metaKey("tail"), oneBinary)
	wb.PutCF(q.cfHandle, q.key(id), data)
	err := q.store.Write(q.store.wo, wb)

	log.Debugf("[Queue] Enqueued data id=%d, err=%v", id, err)
	return id, err
}

func (q *Queue) Dequeue(startId ...uint64) (uint64, []byte, error) {
	store := q.store

	var seekId uint64 = 1
	if len(startId) > 0 {
		seekId = startId[0]
	}
	if seekId < 1 {
		seekId = 1
	}

	var it *rocks.Iterator
	if q.useTailing {
		it = q.tailIterator
		if !it.Valid() {
			// FIXME?(mijia): When Dequeue happens faster than enqueue, the tail iterator would be exhausted
			// so we have seek it again.
			it.Seek(q.key(seekId))
		}
	} else {
		it = store.NewIteratorCF(store.ro, q.cfHandle)
		defer it.Close()
		it.Seek(q.key(seekId))
	}

	if !it.Valid() {
		return 0, nil, EmptyQueue
	}

	wb := rocks.NewWriteBatch()
	defer wb.Destroy()
	key := makeSlice(it.Key())
	value := makeSlice(it.Value())
	wb.DeleteCF(q.cfHandle, key)
	wb.MergeCF(q.cfHandle, q.metaKey("head"), oneBinary)
	err := store.Write(store.wo, wb)
	if err == nil {
		atomic.AddUint64(&q.head, 1)
		if q.useTailing {
			it.Next()
		}
	}

	id := q.id(key)
	log.Debugf("[Queue] Dequeued data id=%d, err=%v", id, err)
	return id, value, err
}

func (q *Queue) EnqueueString(value string) (uint64, error) {
	return q.Enqueue([]byte(value))
}

func (q *Queue) DequeueString(startId ...uint64) (uint64, string, error) {
	id, data, err := q.Dequeue(startId...)
	if err != nil {
		return id, "", err
	}
	return id, string(data), nil
}

func (q *Queue) EnqueueJson(value interface{}) (uint64, error) {
	buf := q.bufPool.Get()
	defer func() {
		buf.Reset()
		q.bufPool.Put(buf)
	}()

	enc := json.NewEncoder(buf)
	if err := enc.Encode(value); err != nil {
		log.Warnf("[Queue] Error when encoding object into json, %s", err)
		return 0, err
	}
	return q.Enqueue(buf.Bytes())
}

func (q *Queue) DequeueJson(value interface{}, startId ...uint64) (uint64, error) {
	id, data, err := q.Dequeue(startId...)
	if err != nil {
		return id, err
	}
	if err := json.Unmarshal(data, value); err != nil {
		log.Warnf("[Queue] Error when decoding json, %s", err)
		return id, err
	}
	return id, nil
}

func (q *Queue) EnqueueGob(value interface{}) (uint64, error) {
	buf := q.bufPool.Get()
	defer func() {
		buf.Reset()
		q.bufPool.Put(buf)
	}()

	enc := gob.NewEncoder(buf)
	if err := enc.Encode(value); err != nil {
		log.Warnf("[Queue] Error when encoding object into gob, %s", err)
		return 0, err
	}
	return q.Enqueue(buf.Bytes())
}

func (q *Queue) DequeueGob(value interface{}, startId ...uint64) (uint64, error) {
	id, data, err := q.Dequeue(startId...)
	if err != nil {
		return id, err
	}

	buf := q.bufPool.Get()
	defer func() {
		buf.Reset()
		q.bufPool.Put(buf)
	}()
	buf.Write(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(value); err != nil {
		log.Warnf("[Queue] Error when decoding gob, %s", err)
		return id, err
	}
	return id, nil
}

func (q *Queue) key(id uint64) []byte {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], id)
	return k[:]
}

func (q *Queue) id(key []byte) uint64 {
	return binary.BigEndian.Uint64(key)
}

func (q *Queue) initQueue() {
	q.head = q.getIndexId("head", 1)
	q.tail = q.getIndexId("tail", 0)
	if q.useTailing {
		store := q.store
		q.tailIterator = store.NewIteratorCF(store.ro, q.cfHandle)
	}
	log.Debugf("[Queue] init queue from store, name=%s, head=%d, tail=%d", q.name, q.head, q.tail)
}

func (q *Queue) getIndexId(name string, defaultValue uint64) uint64 {
	indexId := defaultValue
	if value, err := q.store.GetCF(q.store.ro, q.cfHandle, q.metaKey(name)); err != nil {
		log.Errorf("Failed to get the head key from the rocksdb, %s", err)
	} else {
		sValue := makeSlice(value)
		if len(sValue) > 0 {
			indexId = binary.BigEndian.Uint64(sValue)
		}
	}
	return indexId
}

func (q *Queue) metaKey(sufix string) []byte {
	paddingWidth := 8 // length of a uint64
	oddKey := make([]byte, paddingWidth, paddingWidth+len(sufix))
	oddKey = append(oddKey, []byte(sufix)...)
	return oddKey
}

func newQueue(name string, store *Store, cfHandle *rocks.ColumnFamilyHandle, useTailing bool) *Queue {
	q := &Queue{
		name:       name,
		useTailing: useTailing,
		store:      store,
		cfHandle:   cfHandle,
		bufPool:    bpool.NewBufferPool(64),
	}
	q.initQueue()
	return q
}

func init() {
	oneBinary = make([]byte, 8)
	binary.BigEndian.PutUint64(oneBinary, 1)
}
