## rocksq
An embeded persistent queue based on RocksDB

### Example

```
	store, err := rocksq.NewStore(rocksq.StoreOptions{ 
		Directory:  "/opt/rocksq",
		MemorySize: 5 * 1024 * 1024,
	})
	defer store.Close()
	
	q, err := store.NewQueue("queue_name")
	msg := "Hello World"
	if _, err := q.Enqueue([]byte(msg)); err != nil {
		// ....
	}
	
	id, data, err := q.Dequeue()
	// ....
	
	id, anotherData, err := q.Dequeue(id) // boosting the seek
	if err == rocksq.EmptyQueue {
		break
	}
```

### Benchmark

```
BenchmarkEnqueue-4        300000              7706 ns/op
BenchmarkDequeue-4        100000             30211 ns/op
```

DiableWAL = true

```
BenchmarkEnqueue-4        500000              5163 ns/op
BenchmarkDequeue-4        200000             12365 ns/op
```