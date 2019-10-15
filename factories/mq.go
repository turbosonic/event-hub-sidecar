package factories

import (
	"log"
	"os"

	"github.com/turbosonic/event-hub-sidecar/mq"
	"github.com/turbosonic/event-hub-sidecar/mq/clients/activemq"
	"github.com/turbosonic/event-hub-sidecar/mq/clients/azureservicebus"
	"github.com/turbosonic/event-hub-sidecar/mq/clients/gcpcloudpubsub"
)

// MQClient ...generates a concrete MQClient from environment variables
func MQClient() mq.Client {
	mqc := os.Getenv("MQ_CLIENT")

	switch mqc {
	case "gcpcloudpubsub":
		log.Println("[i] Using GCP Cloud Pub/Sub as a message broker")
		return gcpcloudpubsub.New()
	case "azureservicebus":
		log.Println("[i] Using Azure Service Bus as a message broker")
		return azureservicebus.New()
	default:
		log.Println("[i] using ActiveMQ as a message broker")
		return activemq.New()
	}
}
