package stdout

import (
	"fmt"

	"github.com/turbosonic/event-hub-sidecar/logging"
)

type LoggingClient struct{}

func (l LoggingClient) LogInboundEvent(e logging.InboundEvent) error {
	fmt.Printf("[->]: %+v\n", e)
	return nil
}

func (l LoggingClient) LogOutboundEvent(e logging.OutboundEvent) error {
	fmt.Printf("[<-]: %+v\n", e)
	return nil
}

func New() LoggingClient {
	return LoggingClient{}
}
