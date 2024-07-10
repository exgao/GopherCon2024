package gophercon2024

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"
)

// HandlerFunc is a function type that outlines the structure
// of every recipients' message handling logic
//
// It is invoked by the consumer to deliver the message
type HandlerFunc func(message Message)

type Message struct {
	RoutingKey   string
	Body         string
	ResourceCost int
}

type ResourceManager struct {
	mu                *sync.RWMutex
	AlertChan         chan bool
	LastAlertVal      bool
	MessagesProcessed int
	Utilization       int
}

func NewResourceManager(enableAlerts bool) *ResourceManager {
	rm := &ResourceManager{
		mu:                &sync.RWMutex{},
		AlertChan:         nil,
		MessagesProcessed: 0,
		Utilization:       0,
	}
	if enableAlerts {
		rm.AlertChan = make(chan bool, 4)
	}
	go rm.GenerateRandomLoad()
	return rm
}

func (r *ResourceManager) GenerateRandomLoad() {
	for {
		time.Sleep(50 * time.Millisecond)
		if r.GetUtilization() < 50 {
			cost := 10 + rand.Intn(6)
			r.Add(cost, false)
			go func() {
				time.Sleep(500 * time.Millisecond)
				r.Remove(cost)
			}()
		}
	}
}

func (r *ResourceManager) ProcessMessage(msg Message) {
	// only for output formatting purposes
	if msg.ResourceCost > 9 {
		fmt.Printf("%s\tcost %d \ttotal: %d\n", msg.Body, msg.ResourceCost, r.GetUtilization())
	} else {
		fmt.Printf("%s\tcost %d \t\ttotal: %d\n", msg.Body, msg.ResourceCost, r.GetUtilization())
	}
	r.Add(msg.ResourceCost, true)

	time.Sleep(300 * time.Millisecond)

	go func() {
		val := (500 + time.Duration(rand.Intn(800))) * time.Millisecond
		time.Sleep(val)
		r.Remove(msg.ResourceCost)
	}()
}

func (r *ResourceManager) Add(cost int, incrementCount bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Utilization += cost
	if incrementCount {
		r.MessagesProcessed++
	}
	// if LastAlertVal has changed and alerts are enabled, slow down
	if cap(r.AlertChan) > 0 && r.Utilization >= 80 && !r.LastAlertVal {
		fmt.Printf("-------------------- slowing down --------------------\n")
		r.LastAlertVal = true
		for range cap(r.AlertChan) {
			r.AlertChan <- true
		}
	}
	if r.Utilization >= 100 {
		fmt.Printf("%d messages processed... maximum utilization reached\n", r.MessagesProcessed)
		os.Exit(1)
	}
	return
}

func (r *ResourceManager) Remove(cost int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.Utilization -= cost
	if r.Utilization < 0 {
		r.Utilization = 0
	}
	// if alerts are enabled and LastAlertVal has changed, speed up
	if cap(r.AlertChan) > 0 && r.Utilization < 40 && r.LastAlertVal {
		fmt.Printf("-------------------- speeding up --------------------\n")
		r.LastAlertVal = false
		for range cap(r.AlertChan) {
			r.AlertChan <- false
		}
	}
	return r.Utilization
}

func (r *ResourceManager) GetUtilization() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.Utilization
}
