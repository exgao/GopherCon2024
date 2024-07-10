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
			Topic:   gc.Priority,
			Handler: rm.ProcessMessage,
		},
		{
			Topic:             gc.Normal,
			Handler:           rm.ProcessMessage,
			ThrottlingEnabled: true,
		},
		{
			Topic:             gc.Low,
			Handler:           rm.ProcessMessage,
			ThrottlingEnabled: true,
		},
	}

	m := gc.NewMessenger(queues)

	CriticalProducer := NewProducer(m, gc.Critical, 10)
	PriorityProducer := NewProducer(m, gc.Priority, 10)
	NormalProducer := NewProducer(m, gc.Normal, 10)
	LowProducer := NewProducer(m, gc.Low, 10)

	StartupConsumers(m, rm.AlertChan, queues, wg)

	CriticalProducer.Run()
	time.Sleep(200 * time.Millisecond)
	PriorityProducer.Run()
	time.Sleep(300 * time.Millisecond)
	NormalProducer.Run()
	time.Sleep(150 * time.Millisecond)
	LowProducer.Run()

	// wait until all consumers are done processing
	wg.Wait()

}
