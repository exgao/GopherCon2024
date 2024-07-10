package main

import (
	"fmt"
	gc "gophercon2024"
	"sync"
)

func main() {
	fmt.Println("Hello, GopherCon 2024!")
	wg := &sync.WaitGroup{}

	rm := gc.NewResourceManager(false)

	queues := []gc.Queue{
		{
			Topic:   gc.Critical,
			Handler: rm.ProcessMessage,
		},
		{
			Topic:   gc.Priority,
			Handler: rm.ProcessMessage,
		},
		{
			Topic:   gc.Normal,
			Handler: rm.ProcessMessage,
		},
		{
			Topic:   gc.Low,
			Handler: rm.ProcessMessage,
		},
	}

	m := gc.NewMessenger(queues)

	CriticalProducer := NewProducer(m, gc.Critical, 12)
	PriorityProducer := NewProducer(m, gc.Priority, 11)
	NormalProducer := NewProducer(m, gc.Normal, 6)
	LowProducer := NewProducer(m, gc.Low, 14)

	StartupConsumers(m, queues, wg)

	CriticalProducer.Run()
	PriorityProducer.Run()
	NormalProducer.Run()
	LowProducer.Run()

	// wait until all consumers are done processing
	wg.Wait()

}
