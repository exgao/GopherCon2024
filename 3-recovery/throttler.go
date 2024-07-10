package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type adjustableDelay struct {
	val time.Duration
	mu  sync.RWMutex
}

// adjust spins off a goroutine to update the throttler's current delay value based on the configured multiplier and Interval
// it runs until the throttler's context is cancelled or maxDelay is reached, whichever happens first
func (a *adjustableDelay) adjust(interval time.Duration, multiplier float64, maxDelay time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				a.mu.Lock()
				a.val = time.Duration(float64(a.val.Milliseconds())*multiplier) * time.Millisecond
				if multiplier > 1 && a.val.Milliseconds() >= maxDelay.Milliseconds() {
					a.val = maxDelay
					a.mu.Unlock()
					return
				} else if multiplier < 1 && a.val.Milliseconds() <= 50 {
					a.val = 0
					a.mu.Unlock()
					return
				}
				a.mu.Unlock()
			}
		}
	}()
}

func (a *adjustableDelay) getVal() time.Duration {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.val
}

// *** Block throttle ***

type Block struct {
	mu *sync.RWMutex
	// user defined
	recoveryDelay      time.Duration
	recoveryIncrement  time.Duration
	recoveryMultiplier float64

	// internal
	delay         adjustableDelay
	throttleStart int64
	recoveryStart int64
	lastTimestamp time.Time
}

func (b *Block) Validate() error {
	if b == nil {
		return nil
	}
	// recovery values
	if b.recoveryDelay.Milliseconds() < 50 || b.recoveryDelay.Hours() > 8 {
		return fmt.Errorf("recoveryDelay %s must be at least 50 ms and less than 8 hours", b.recoveryDelay.String())
	}
	if b.recoveryIncrement.Milliseconds() < 50 {
		return fmt.Errorf("recoveryIncrement %s must be at least 50 ms", b.recoveryIncrement.String())
	}
	if b.recoveryMultiplier <= 0 || b.recoveryMultiplier >= 1 {
		return fmt.Errorf("recoveryMultiplier %f must be greater than 0 and less than 1", b.recoveryMultiplier)
	}
	return nil
}

func (b *Block) InitThrottle() {
	b.recoveryStart = 0
	b.throttleStart = time.Now().Unix()
}

func (b *Block) InitRecovery() {
	b.throttleStart = 0
	b.recoveryStart = time.Now().Unix()
	b.delay.val = b.recoveryDelay

	b.delay.adjust(b.recoveryIncrement, b.recoveryMultiplier, 0)
}

func (b *Block) IsThrottling() bool {
	return b.throttleStart > 0
}

func (b *Block) IsRecovering() bool {
	return b.recoveryStart > 0 && b.delay.getVal() > 0
}

func (b *Block) Apply(ctx context.Context) {
	if b.IsThrottling() {
		// throttling mode, block until context canceled
		<-ctx.Done()
	} else if b.IsRecovering() {
		t := time.NewTimer(b.delay.getVal())
		defer t.Stop()
		// recovery mode
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			fmt.Printf("block waited %s\n", b.delay.getVal().String())
			return
		}
	}
}

func NewBlockThrottler(recoveryDelay time.Duration, recoveryIncrement time.Duration, recoveryMultiplier float64) *Block {
	return &Block{
		mu:                 &sync.RWMutex{},
		recoveryDelay:      recoveryDelay,
		recoveryIncrement:  recoveryIncrement,
		recoveryMultiplier: recoveryMultiplier,
	}
}

// *** Interval throttle ***

type Interval struct {
	// user input
	throttleDelay      time.Duration
	recoveryDelay      time.Duration
	recoveryIncrement  time.Duration
	recoveryMultiplier float64

	// internal
	delay         adjustableDelay
	throttleStart int64
	recoveryStart int64
	lastTimestamp time.Time
}

func (i *Interval) Validate() error {
	if i == nil {
		return nil
	}
	// throttle values
	if i.throttleDelay.Milliseconds() < 50 {
		return fmt.Errorf("delay %s must be at least 50 ms", i.throttleDelay.String())
	}

	// recovery values
	if i.recoveryIncrement.Milliseconds() < 50 {
		return fmt.Errorf("recoveryIncrement %s must be at least 50 ms", i.recoveryIncrement.String())
	}
	if i.recoveryMultiplier <= 0 || i.recoveryMultiplier >= 1 {
		return fmt.Errorf("recoveryMultiplier %f must be between 0 and 1", i.recoveryMultiplier)
	}
	return nil
}

func (i *Interval) InitThrottle() {
	i.recoveryStart = 0
	i.throttleStart = time.Now().Unix()
	i.delay.val = i.throttleDelay
}

func (i *Interval) InitRecovery() {
	i.throttleStart = 0
	i.delay.val = i.throttleDelay
	i.recoveryStart = time.Now().Unix()

	i.delay.adjust(i.recoveryIncrement, i.recoveryMultiplier, 0)
}

func (i *Interval) IsThrottling() bool {
	return i.throttleStart > 0
}

func (i *Interval) IsRecovering() bool {
	return i.recoveryStart > 0 && i.delay.getVal() > 0
}

func (i *Interval) Apply(ctx context.Context) {
	d := i.delay.getVal()
	if d == 0 {
		//time.Sleep(250 * time.Millisecond)
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return
	case <-t.C:
		fmt.Printf("interval waited %s\n", d.String())
		return
	}
}

func NewIntervalThrottler(throttleDelay time.Duration, recoveryIncrement time.Duration, recoveryMultiplier float64) *Interval {
	return &Interval{
		throttleDelay:      throttleDelay,
		recoveryIncrement:  recoveryIncrement,
		recoveryMultiplier: recoveryMultiplier,
	}
}

// *** Backoff throttle ***

type Backoff struct {
	// user input
	initialDelay       time.Duration
	backoffInterval    time.Duration
	multiplier         float64
	maxDelay           time.Duration
	recoveryDelay      time.Duration
	recoveryIncrement  time.Duration
	recoveryMultiplier float64

	// internal
	throttleStart int64
	delay         adjustableDelay
	recoveryStart int64
	lastTimestamp time.Time
}

func (bo *Backoff) Validate() error {
	if bo == nil {
		return nil
	}
	// throttle values
	if bo.initialDelay.Milliseconds() < 50 {
		return fmt.Errorf("initialDelay %s must be at least 50 ms", bo.initialDelay.String())
	}
	if bo.backoffInterval.Milliseconds() < 50 {
		return fmt.Errorf("backoffInterval %s must be at least 50 ms", bo.backoffInterval.String())
	}
	if bo.multiplier <= 1 {
		return fmt.Errorf("multiplier %f must be greater than 1", bo.multiplier)
	}
	// maximum value 8 hrs
	if bo.maxDelay.Milliseconds() < 50 || bo.maxDelay.Hours() > 8 {
		return fmt.Errorf("maxDelay %s must be greater than 50 ms and less than 8 hours", bo.maxDelay.String())
	}
	if bo.initialDelay >= bo.maxDelay {
		return fmt.Errorf("maxDelay %s must be greater than initialDelay %s", bo.maxDelay.String(), bo.initialDelay.String())
	}
	return nil
}

func (bo *Backoff) InitThrottle() {
	bo.recoveryStart = 0
	bo.throttleStart = time.Now().Unix()
	bo.delay.val = bo.initialDelay
	bo.delay.adjust(bo.backoffInterval, bo.multiplier, bo.maxDelay)
}

func (bo *Backoff) InitRecovery() {
	bo.throttleStart = 0
	bo.recoveryStart = time.Now().Unix()
	bo.delay.val = min(bo.delay.getVal(), bo.maxDelay)

	bo.delay.adjust(bo.backoffInterval, 1/bo.multiplier, 0)
}

func (bo *Backoff) IsThrottling() bool {
	return bo.throttleStart > 0
}

func (bo *Backoff) IsRecovering() bool {
	return bo.recoveryStart > 0 && bo.delay.getVal() > 0
}

func (bo *Backoff) Apply(ctx context.Context) {
	d := bo.delay.getVal()
	if d == 0 {
		//time.Sleep(250 * time.Millisecond)
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return
	case <-t.C:
		fmt.Printf("backoff waited %s\n", d.String())
		return
	}
}

func NewBackoffThrottler(initialDelay time.Duration, backoffInterval time.Duration, multiplier float64, maxDelay time.Duration) *Backoff {
	var recoveryMultiplier float64
	if multiplier > 0 {
		recoveryMultiplier = 1 / multiplier
	}
	return &Backoff{
		initialDelay:       initialDelay,
		backoffInterval:    backoffInterval,
		multiplier:         multiplier,
		maxDelay:           maxDelay,
		recoveryDelay:      maxDelay,
		recoveryIncrement:  backoffInterval,
		recoveryMultiplier: recoveryMultiplier,
	}
}
