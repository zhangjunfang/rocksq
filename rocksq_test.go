package rocksq

import (
	"fmt"
	"testing"
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

	for {
		id, data, err := q.Dequeue()
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
