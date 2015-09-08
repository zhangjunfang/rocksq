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
BenchmarkEnqueue-4        500000              6798 ns/op
BenchmarkDequeue-4        200000             16434 ns/op
```

DiableWAL = true

```
BenchmarkEnqueue-4        500000              4983 ns/op
BenchmarkDequeue-4        200000             17159 ns/op
```

SetFillCache = false, DisableWAL = true

```
BenchmarkEnqueue-4        500000              4623 ns/op
BenchmarkDequeue-4        300000             10098 ns/op
```

DisableWAL = true, UseTailing = true

```
BenchmarkEnqueue-4        500000              4690 ns/op
BenchmarkDequeue-4        500000              6108 ns/op
```

PS: Tailing iterator really helps a lot to reduce the seek cost, but the test case maybe not that realistic since only very few seek happens.