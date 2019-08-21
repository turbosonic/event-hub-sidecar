package logging

import (
	"fmt"
	"os"
	"time"

	"github.com/turbosonic/event-hub-sidecar/dto"
	"github.com/turbosonic/event-hub-sidecar/variables"
)

var podName = os.Getenv("HOSTNAME")

// the expected interface of a logging client
type Client interface {
	LogInboundEvent(InboundEvent) error
	LogOutboundEvent(OutboundEvent) error
}

// the object for an event coming from the event hub
type InboundEvent struct {
	ID           string
	Name         string
	Microservice string
	PodName      string
	Duration     int64
	Processed    time.Time
	Attempts     int64
	Status       dto.HandledEventStatus
}

// the object for an event going to the event hub
type OutboundEvent struct {
	ID           string
	Name         string
	Microservice string
	PodName      string
	Attempts     int64
	Sent         time.Time
	Status       dto.SentEventStatus
}

// the object used by the rest of the code to log
type Handler struct {
	client Client
}

// instantiating a new handler with a client
func New(c Client) Handler {
	return Handler{
		client: c,
	}
}

// logging an inbound event
func (h *Handler) LogInboundEvent(e *dto.Event, s dto.HandledEventStatus, v *variables.Data, duration int64, attempts int64) {
	i := InboundEvent{
		ID:           e.ID,
		Microservice: v.MicroserviceName,
		PodName:      podName,
		Duration:     duration,
		Attempts:     attempts,
		Name:         e.Name,
		Processed:    time.Now(),
		Status:       s,
	}

	err := h.client.LogInboundEvent(i)
	if err != nil {
		fmt.Printf("[!] Inbound event with ID %s could not be logged\n", e.ID)
	}
}

// logging an outbound event
func (h *Handler) LogOutboundEvent(e *dto.Event, s dto.SentEventStatus, v *variables.Data, attempts int64) {
	i := OutboundEvent{
		ID:           e.ID,
		Microservice: v.MicroserviceName,
		PodName:      podName,
		Attempts:     attempts,
		Name:         e.Name,
		Sent:         time.Now(),
		Status:       s,
	}

	err := h.client.LogOutboundEvent(i)
	if err != nil {
		fmt.Printf("[!] Outbound event could not be logged\n")
	}
}
