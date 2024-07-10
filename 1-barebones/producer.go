package main

import (
	"fmt"
	gc "gophercon2024"
	"math/rand"
)

type Producer struct {
	Publisher   Publisher
	topic       string
	resourceMax int
}

type Publisher interface {
	Publish(topic string, msg gc.Message)
	CloseChannel(topic string)
}

func NewProducer(messenger Publisher, topic string, resourceMax int) *Producer {
	return &Producer{
		Publisher:   messenger,
		topic:       topic,
		resourceMax: resourceMax,
	}
}

func (p *Producer) Run() {
	go func() {
		for range 60 {
			body := fmt.Sprintf("[%s]", p.topic)
			if p.topic == gc.Low {
				body = fmt.Sprintf("%s\t", body)
			}
			p.Publisher.Publish(p.topic, gc.Message{
				Body:         body,
				ResourceCost: rand.Intn(p.resourceMax),
			})
		}
		// close delivery channel when all messages have been sent
		p.Close()
	}()
}

func (p *Producer) Close() {
	p.Publisher.CloseChannel(p.topic)
}
