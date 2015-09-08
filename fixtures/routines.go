package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/mijia/rocksq"
)

func main() {
	var count int
	flag.IntVar(&count, "c", 1000, "Enqueue counts")
	flag.Parse()

	opts := rocksq.StoreOptions{
		Directory:             "/opt/rocksq_fixtures",
		MemorySize:            25 * 1024 * 1024,
		DisableAutoCompaction: true,
		DisableWAL:            true,
	}
	store, err := rocksq.NewStore(opts)
	if err != nil {
		panic(err)
	}
	defer store.Destroy()

	q, err := store.NewQueue("test_queue")
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		start := time.Now()
		errCount := 0
		for i := 0; i < count; i += 1 {
			if _, err := q.EnqueueString("Hello World"); err != nil {
				errCount += 1
			}
		}
		duration := time.Since(start)
		unit := time.Duration(int64(duration) / int64(count))
		fmt.Printf("Enqueue done, duration=%s, avgDuration=%s, errCount=%d\n", duration, unit, errCount)
		wg.Done()
	}()

	go func() {
		start := time.Now()
		errCount := 0
		waitCount := 0
		orderCount := 0
		gotCount := 0
		var startId uint64 = 1
		for {
			id, _, err := q.DequeueString(startId)
			if id != startId+1 {
				orderCount += 1
			}
			startId = id
			if err != nil && err != rocksq.EmptyQueue {
				errCount += 1
			} else if err == rocksq.EmptyQueue {
				waitCount += 1
				time.Sleep(5 * time.Millisecond)
			} else {
				gotCount += 1
				if gotCount >= count {
					break
				}
			}
		}
		duration := time.Since(start)
		unit := time.Duration((int64(duration) - 5*int64(waitCount)*1e6) / int64(count))
		fmt.Printf("Dequeue done, duration=%s, avgDuration=%s, errCount=%d, waitCoun=%d, orderCount=%d\n", duration, unit, errCount, waitCount, orderCount)
		wg.Done()
	}()

	wg.Wait()
}
