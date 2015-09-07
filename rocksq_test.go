package rocksq

import (
	"encoding/gob"
	"fmt"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	store, err := NewStore(StoreOptions{
		Directory:  "/opt/rocksq",
		MemorySize: 5 * 1024 * 1024,
		IsDebug:    false,
	})
	if err != nil {
		t.Fatalf("Failed to create the rocksdb store, %s", err)
	}
	defer store.Destroy()

	q, err := store.NewQueue("q1")
	if err != nil {
		t.Fatalf("Failed to create the queue, %s", err)
	}

	for i := 0; i < 10; i += 1 {
		value := "Hello World"
		if _, err := q.Enqueue([]byte(value)); err != nil {
			t.Fatalf("Failed to enqueue the data, %s", err)
		}
	}
	fmt.Printf("Queue message left: %d\n", q.ApproximateSize())

	var startId uint64 = 1
	for {
		id, data, err := q.Dequeue(startId)
		startId = id
		if err != nil {
			if err != EmptyQueue {
				t.Fatalf("Failed to dequeue dta, %s", err)
			}
			break
		}
		value := string(data)
		if value != "Hello World" {
			t.Errorf("Data corrupted")
		}
		fmt.Printf("Dequeue: id=%d, value=%s\n", id, value)
	}
}

type Point struct {
	X, Y int
}

func TestEncodingJson(t *testing.T) {
	store, err := NewStore(StoreOptions{
		Directory:  "/opt/rocksq",
		MemorySize: 5 * 1024 * 1024,
		IsDebug:    false,
	})
	if err != nil {
		t.Fatalf("Failed to create the rocksdb store, %s", err)
	}
	defer store.Destroy()

	q, err := store.NewQueue("q1")
	if err != nil {
		t.Fatalf("Failed to create the queue, %s", err)
	}
	for i := 0; i < 10; i += 1 {
		if _, err := q.EnqueueJson(time.Now()); err != nil {
			t.Fatalf("Failed to enqueue the json object, %s", err)
		}
	}
	fmt.Printf("Queue message left: %d\n", q.ApproximateSize())

	var startId uint64 = 1
	for {
		var value time.Time
		id, err := q.DequeueJson(&value, startId)
		startId = id
		if err != nil {
			if err != EmptyQueue {
				t.Fatalf("Failed to dequeue json object, %s", err)
			}
			break
		}
		fmt.Printf("Dequeue JSON: id=%d, value=%v\n", id, value)
	}
}

func TestEncodingGob(t *testing.T) {
	store, err := NewStore(StoreOptions{
		Directory:  "/opt/rocksq",
		MemorySize: 5 * 1024 * 1024,
		IsDebug:    false,
	})
	if err != nil {
		t.Fatalf("Failed to create the rocksdb store, %s", err)
	}
	defer store.Destroy()

	q, err := store.NewQueue("q1")
	if err != nil {
		t.Fatalf("Failed to create the queue, %s", err)
	}
	for i := 0; i < 10; i += 1 {
		p := Point{i, i * 2}
		if _, err := q.EnqueueGob(p); err != nil {
			t.Fatalf("Failed to enqueue the gob object, %s", err)
		}
	}
	fmt.Printf("Queue message left: %d\n", q.ApproximateSize())

	var startId uint64 = 1
	for {
		var value Point
		id, err := q.DequeueGob(&value, startId)
		startId = id
		if err != nil {
			if err != EmptyQueue {
				t.Fatalf("Failed to dequeue gob object, %s", err)
			}
			break
		}
		fmt.Printf("Dequeue GOB: id=%d, value=%v\n", id, value)
	}

}

func init() {
	gob.Register(Point{})
}
