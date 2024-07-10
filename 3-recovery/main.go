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
			Topic:   gc.Critical,
			Handler: rm.ProcessMessage,
		},
		{
			Topic:         gc.Priority,
			Handler:       rm.ProcessMessage,
			FullThrottler: NewIntervalThrottler(150*time.Millisecond, 300*time.Millisecond, 0.5),
		},
		{
			Topic:   gc.Normal,
			Handler: rm.ProcessMessage,
			FullThrottler: NewBackoffThrottler(
				100*time.Millisecond,
				250*time.Millisecond,
				2,
				350*time.Millisecond,
			),
		},
		{
			Topic:         gc.Low,
			Handler:       rm.ProcessMessage,
			FullThrottler: NewBlockThrottler(250*time.Millisecond, 350*time.Millisecond, 0.5),
		},
	}

	m := gc.NewMessenger(queues)

	CriticalProducer := NewProducer(m, gc.Critical, 8)
	PriorityProducer := NewProducer(m, gc.Priority, 12)
	NormalProducer := NewProducer(m, gc.Normal, 10)
	LowProducer := NewProducer(m, gc.Low, 11)

	StartupConsumers(m, rm.AlertChan, queues, wg)

	CriticalProducer.Run()
	PriorityProducer.Run()
	NormalProducer.Run()
	LowProducer.Run()

	// wait until all consumers are done processing
	wg.Wait()

}
