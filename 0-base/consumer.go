package main

import (
	"fmt"
	gc "gophercon2024"
	"sync"
	"time"
)

type messenger interface {
	GetChannels() map[string]<-chan gc.Message
}

type Consumer struct {
	wg *sync.WaitGroup

	Name       string
	Deliveries <-chan gc.Message
	Handler    gc.HandlerFunc
}

func StartupConsumers(m messenger, queues []gc.Queue, wg *sync.WaitGroup) {
	channels := m.GetChannels()
	for _, queue := range queues {
		time.Sleep(50 * time.Millisecond)
		c := &Consumer{
			Name:       queue.Topic,
			Deliveries: channels[queue.Topic],
			wg:         wg,
			Handler:    queue.Handler,
		}
		wg.Add(1)
		go c.Consume(c.Deliveries, c.Handler)
	}
}

func (c *Consumer) Consume(deliveryChan <-chan gc.Message, handler gc.HandlerFunc) {
	defer c.wg.Done()
	for {
		d, ok := <-deliveryChan
		// stop consumer if channel has been closed
		if !ok {
			fmt.Printf("All %s messages received!\n", c.Name)
			return
		}

		// invoke handlerFunc
		handler(d)
	}
}
