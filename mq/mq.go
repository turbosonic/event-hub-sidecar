package mq

import "event-delivery-sidecar/dto"

type MQ struct {
	Client Client
}

type eventFunction func(dto.Event) dto.HandledEventStatus

// Client ...an common interface for all message brokers
type Client interface {

	// handles the connections and reconnections to a message broker and the receiving of events
	Listen(eventFunction)

	// handles the sending of events
	Send(*dto.Event) error
}
