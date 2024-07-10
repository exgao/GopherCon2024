package main

import (
	"fmt"
	"sync"
	"time"
)

type adjustableDelay struct {
	val time.Duration
	mu  sync.RWMutex
}

// adjust spins off a goroutine to update the throttler's current delay
// based on the configured multiplier and interval
func (a *adjustableDelay) adjust(interval time.Duration, multiplier float64, maxDelay time.Duration) {
	t := time.NewTicker(interval)
	for {
		<-t.C
		a.val = time.Duration(float64(a.val.Milliseconds())*multiplier) * time.Millisecond
		if (multiplier > 1 && a.val.Milliseconds() >= maxDelay.Milliseconds()) ||
			(multiplier < 1 && a.val.Milliseconds() <= 50) {
			t.Stop()
			return
		}
	}
}

func (a *adjustableDelay) getVal() time.Duration {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.val
}

// *** Block throttle ***

type block struct{}

func (b *block) InitThrottle() {}

func (b *block) Apply() {
	//https://go.dev/ref/spec#Select_statements
	//
	//From go language spec: Since communication on nil channels can never proceed,
	//a select with only nil channels and no default case blocks forever.
	select {}
}

// *** Interval throttle ***

type interval struct {
	throttleDelay time.Duration

	// internal
	delay adjustableDelay
}

func (i *interval) InitThrottle() {
	i.delay.val = i.throttleDelay
}

func (i *interval) Apply() {
	fmt.Printf("Apply interval %dms\n", i.delay.getVal().Milliseconds())
	time.Sleep(i.delay.getVal())
}

// *** Backoff throttle ***

type backoff struct {
	// user input
	initialDelay    time.Duration
	backoffInterval time.Duration
	multiplier      float64
	maxDelay        time.Duration

	// internal
	throttleStart int64
	delay         adjustableDelay
}

func (bo *backoff) InitThrottle() {
	bo.delay.val = bo.initialDelay
	go bo.delay.adjust(bo.backoffInterval, bo.multiplier, bo.maxDelay)
}

func (bo *backoff) Apply() {
	fmt.Printf("Apply backoff %dms\n", bo.delay.getVal().Milliseconds())
	time.Sleep(bo.delay.getVal())
}
