package mq

import (
	"github.com/turbosonic/event-hub-sidecar/dto"
	"github.com/turbosonic/event-hub-sidecar/variables"
)

type MQ struct {
	Client Client
}

type EventFunction func(dto.Event) dto.HandledEventStatus

// Client ...an common interface for all message brokers
type Client interface {

	// handles the connections and reconnections to a message broker and the receiving of events
	// requires three channels, one for queue events, one for topic events and one for reporting message broker connects and disconnects
	Listen(chan dto.Event, chan dto.Event, chan bool, *variables.Data)

	// handles the sending of events
	Send(*dto.Event) error

	// handles termination requests
	Terminate() error
}
