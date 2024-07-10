package main

import (
	"fmt"
	gc "gophercon2024"
	"math/rand"
)

func NewProducer(messenger Publisher, topic string, resourceMax int) *Producer {
	return &Producer{
		Publisher:   messenger,
		topic:       topic,
		resourceMax: resourceMax,
	}
}

type Publisher interface {
	Publish(topic string, msg gc.Message)
}

type Producer struct {
	Publisher   Publisher
	topic       string
	resourceMax int
}

func (p *Producer) Run() {
	go func() {
		for {
			body := fmt.Sprintf("[%s]", p.topic)
			if p.topic == gc.Low {
				body = fmt.Sprintf("%s\t", body)
			}
			p.Publisher.Publish(p.topic, gc.Message{
				Body:         body,
				ResourceCost: rand.Intn(p.resourceMax),
			})
		}
	}()
}
