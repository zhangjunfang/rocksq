package rocksq

import "testing"

var store *Store

func BenchmarkEnqueue(b *testing.B) {
	q, err := store.NewQueue("ad2")
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i += 1 {
		if _, err := q.Enqueue([]byte("Hello World")); err != nil {
			panic(err)
		}
	}
}

func BenchmarkDequeue(b *testing.B) {
	q, err := store.NewQueue("ad2")
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	var startId uint64 = 1
	for i := 0; i < b.N; i += 1 {
		id, _, err := q.Dequeue(startId)
		if err != nil && err != EmptyQueue {
			panic(err)
		}
		startId = id
	}
}

func init() {
	var err error
	store, err = NewStore(StoreOptions{
		Directory:  "/opt/rocksq2",
		MemorySize: 5 * 1024 * 1024,
		IsDebug:    false,
	})
	if err != nil {
		panic(err)
	}
}
