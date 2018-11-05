package dto

import (
	"encoding/json"
	"time"
)

type HandledEventStatus int

const (
	Accepted = 1 + iota
	Rejected
	Released
)

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
