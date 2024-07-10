package main

import (
	"context"
	"fmt"
	gc "gophercon2024"
	"os"
	"sync"
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
	Apply(ctx context.Context)
	InitThrottle()
	InitRecovery()
	Validate() error
	IsThrottling() bool
	IsRecovering() bool
}

func StartupConsumers(m messenger, alertChan <-chan bool, queues []gc.Queue, wg *sync.WaitGroup) {
	channels := m.GetChannels()
	for _, queue := range queues {
		c := &Consumer{
			Name:        queue.Topic,
			Deliveries:  channels[queue.Topic],
			wg:          wg,
			mu:          &sync.RWMutex{},
			alertActive: false,
			throttle:    queue.FullThrottler,
			Handler:     queue.Handler,
		}
		wg.Add(1)

		if c.throttle != nil {
			err := c.throttle.Validate()
			if err != nil {
				fmt.Printf("invalid throttler configuration: %s", err)
				os.Exit(1)
			}
		}

		go c.Consume(c.Deliveries, alertChan, c.Handler)
	}
}

func (c *Consumer) Consume(deliveryChan <-chan gc.Message, alertChan <-chan bool, handler gc.HandlerFunc) {
	defer c.wg.Done()
	processed := make(chan bool)
	for {
		d, ok := <-deliveryChan
		// stop consumer if channel has been closed
		if !ok {
			fmt.Printf("All %s messages received!\n", c.Name)
			return
		}
		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			select {
			case <-processed:
				cancel()
				return
			case alert := <-alertChan:
				cancel()
				c.SetAlertVal(alert)
				<-processed
				return
			}
		}()

		c.Throttle(ctx)
		// invoke handlerFunc
		handler(d)
		processed <- true
	}
}

func (c *Consumer) Throttle(ctx context.Context) {
	if c.throttle == nil {
		return
	}
	if c.GetAlertVal() && !c.throttle.IsThrottling() {
		c.throttle.InitThrottle()
	} else if !c.GetAlertVal() && c.throttle.IsThrottling() {
		c.throttle.InitRecovery()
	}

	c.throttle.Apply(ctx)
}

func (c *Consumer) GetAlertVal() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.alertActive
}

func (c *Consumer) SetAlertVal(val bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.alertActive = val
}
