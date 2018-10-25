package factories

import (
	"log"
	"os"

	"event-delivery-sidecar/mq"
	"event-delivery-sidecar/mq/clients/activemq"
)

// MQClient ...generates a concrete MQClient from environment variables
func MQClient() mq.Client {
	mqc := os.Getenv("MQ_CLIENT")

	switch mqc {
	// case "azureservicebus":
	// 	log.Println("[x] Using Azure Service Bus as a message broker")
	// 	return azureservicebus.New()
	default:
		log.Println("[x] Using ActiveMQ as a message broker")
		return activemq.New()
	}
}
