package factories

import (
	"log"
	"os"

	"github.com/turbosonic/event-hub-sidecar/mq"
	"github.com/turbosonic/event-hub-sidecar/mq/clients/activemq"
	"github.com/turbosonic/event-hub-sidecar/mq/clients/azureservicebus"
)

// MQClient ...generates a concrete MQClient from environment variables
func MQClient() mq.Client {
	mqc := os.Getenv("MQ_CLIENT")

	switch mqc {
	case "azureservicebus":
		log.Println("[i] Using Azure Service Bus as a message broker")
		return azureservicebus.New()
	default:
		log.Println("[i] using ActiveMQ as a message broker")
		return activemq.New()
	}
}
