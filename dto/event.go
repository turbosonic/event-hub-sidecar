package dto

import (
	"encoding/json"
	"time"
)

type HandledEventStatus int

const (
	Accepted HandledEventStatus = 1 + iota
	Rejected
	Released
)

func (s HandledEventStatus) String() string {
	return [...]string{"Accepted", "Rejected", "Released"}[s-1]
}

type SentEventStatus int

const (
	Failed SentEventStatus = 1 + iota
	Succeeded
)

func (s SentEventStatus) String() string {
	return [...]string{"Failed", "Succeeded"}[s-1]
}

// Event ...an incomming event object
type Event struct {
	ID            string      `json:"id"`
	Name          string      `json:"name"`
	Source        string      `json:"source"`
	Timestamp     time.Time   `json:"timestamp"`
	Handled       time.Time   `json:"handled_timestamp"`
	Payload       interface{} `json:"payload"`
	RequestID     string      `json:"request_id"`
	HandledStatus *chan HandledEventStatus
}

// NewEventFromByteArray .. creates an event from a ByteArray
func NewEventFromByteArray(bytes *[]byte) (Event, error) {
	e := Event{}
	err := json.Unmarshal(*bytes, &e)
	return e, err
}

// ToByteArray ... Marshals the event into a ByteArray
func (e *Event) ToByteArray() ([]byte, error) {
	return json.Marshal(e)
}
