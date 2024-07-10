package gophercon2024

import (
	"context"
	"fmt"
)

type (
	BasicThrottler interface {
		Apply()
		InitThrottle()
	}

	Throttler interface {
		Validate() error
		Apply(ctx context.Context)
		InitThrottle()
		InitRecovery()
		IsThrottling() bool
		IsRecovering() bool
	}

	Queue struct {
		Topic             string
		Key               string
		ThrottlingEnabled bool
		BasicThrottler    BasicThrottler
		FullThrottler     Throttler
		Handler           HandlerFunc
	}
)

const (
	Critical = "Critical"
	Priority = "Priority"
	Normal   = "Normal"
	Low      = "Low"
)

func NewMessenger(queues []Queue) *Messenger {
	m := &Messenger{
		queues:          queues,
		CriticalChan:    make(chan Message, 100),
		PriorityChan:    make(chan Message, 100),
		NormalChan:      make(chan Message, 100),
		LowPriorityChan: make(chan Message, 100),
	}

	return m
}

func (m *Messenger) GetChannels() map[string]<-chan Message {
	return map[string]<-chan Message{
		Critical: m.CriticalChan,
		Priority: m.PriorityChan,
		Normal:   m.NormalChan,
		Low:      m.LowPriorityChan,
	}
}

func (m *Messenger) CloseChannel(topic string) {
	switch topic {
	case Critical:
		close(m.CriticalChan)
	case Priority:
		close(m.PriorityChan)
	case Normal:
		close(m.NormalChan)
	case Low:
		close(m.LowPriorityChan)
	}
}

type Messenger struct {
	queues []Queue

	// channels
	CriticalChan    chan Message
	PriorityChan    chan Message
	NormalChan      chan Message
	LowPriorityChan chan Message
}

func (m *Messenger) Publish(topic string, msg Message) {
	if m == nil {
		fmt.Printf("queue not found")
		return
	}

	switch topic {
	case Critical:
		m.CriticalChan <- msg
	case Priority:
		m.PriorityChan <- msg
	case Normal:
		m.NormalChan <- msg
	case Low:
		m.LowPriorityChan <- msg
	}
}
