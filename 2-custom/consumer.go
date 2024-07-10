package main

import (
	"fmt"
	gc "gophercon2024"
	"sync"
	"time"
)

type Consumer struct {
	m  messenger
	wg *sync.WaitGroup

	mu          *sync.RWMutex
	alertActive bool
	throttle    Throttler

	Name       string
	Deliveries <-chan gc.Message
	Handler    gc.HandlerFunc
}

type messenger interface {
	GetChannels() map[string]<-chan gc.Message
}

type Throttler interface {
	Apply()
	InitThrottle()
}

func StartupConsumers(m messenger, alertChan <-chan bool, queues []gc.Queue, wg *sync.WaitGroup) {
	channels := m.GetChannels()
	for _, queue := range queues {
		time.Sleep(60 * time.Millisecond)
		c := &Consumer{
			Name:        queue.Topic,
			Deliveries:  channels[queue.Topic],
			wg:          wg,
			mu:          &sync.RWMutex{},
			alertActive: false,
			throttle:    queue.BasicThrottler,
			Handler:     queue.Handler,
		}
		wg.Add(1)
		go c.Consume(c.Deliveries, c.Handler)
		go c.Watch(alertChan)
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

		if c.alertActive && c.throttle != nil {
			c.throttle.Apply()
		}

		// invoke handlerFunc
		handler(d)
	}
}

func (c *Consumer) Watch(alertChan <-chan bool) {
	for {
		alert := <-alertChan
		c.mu.Lock()
		c.alertActive = alert
		if c.throttle != nil {
			c.throttle.InitThrottle()
		}
		c.mu.Unlock()
	}
}
