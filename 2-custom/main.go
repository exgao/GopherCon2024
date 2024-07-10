package main

import (
	"fmt"
	gc "gophercon2024"
	"sync"
	"time"
)

func main() {
	fmt.Println("Hello, GopherCon 2024!")
	wg := &sync.WaitGroup{}

	rm := gc.NewResourceManager(true)

	queues := []gc.Queue{
		{
			Topic:   gc.Priority,
			Handler: rm.ProcessMessage,
			BasicThrottler: &interval{
				throttleDelay: 100 * time.Millisecond,
			},
		},
		{
			Topic:   gc.Normal,
			Handler: rm.ProcessMessage,
			BasicThrottler: &backoff{
				initialDelay:    50 * time.Millisecond,
				backoffInterval: 500 * time.Millisecond,
				multiplier:      2,
				maxDelay:        2 * time.Second,
			},
		},
		{
			Topic:          gc.Low,
			Handler:        rm.ProcessMessage,
			BasicThrottler: &block{},
		},
	}

	m := gc.NewMessenger(queues)

	CriticalProducer := NewProducer(m, gc.Critical, 10)
	PriorityProducer := NewProducer(m, gc.Priority, 10)
	NormalProducer := NewProducer(m, gc.Normal, 10)
	LowProducer := NewProducer(m, gc.Low, 10)

	StartupConsumers(m, rm.AlertChan, queues, wg)

	CriticalProducer.Run()
	PriorityProducer.Run()
	NormalProducer.Run()
	LowProducer.Run()

	done := make(chan bool)
	go func() {
		// wait until all consumers are done processing
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("All messages processed")
	case <-time.After(15 * time.Second):
		fmt.Println("At least one consumer is stuck :(")
	}
}
