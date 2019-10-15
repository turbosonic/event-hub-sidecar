package applicationinsights

import (
	"log"
	"os"

	"github.com/Microsoft/ApplicationInsights-Go/appinsights"
	"github.com/turbosonic/event-hub-sidecar/logging"
)

type Client struct {
	aiClient appinsights.TelemetryClient
}

func New() Client {

	aiInsKey := os.Getenv("APPLICATIONINSIGHTS_INTRUMENTATION_KEY")
	if aiInsKey == "" {
		log.Panic("[!!!] No APPLICATIONINSIGHTS_INTRUMENTATION_KEY found in the environment")
	}

	client := appinsights.NewTelemetryClient(aiInsKey)

	return Client{
		aiClient: client,
	}
}

func (client Client) LogInboundEvent(ev logging.InboundEvent) error {

	event := appinsights.NewEventTelemetry(ev.Name)
	event.Timestamp = ev.Processed

	event.Properties["ID"] = ev.ID
	event.Properties["Type"] = "Inbound event"
	event.Properties["Status"] = ev.Status.String()

	event.Properties["Microservice"] = ev.Microservice
	event.Properties["Pod"] = ev.PodName

	event.Measurements["Duration"] = float64(ev.Duration)
	event.Measurements["Attempts"] = float64(ev.Attempts)

	client.aiClient.Track(event)

	return nil
}

func (client Client) LogOutboundEvent(ev logging.OutboundEvent) error {
	event := appinsights.NewEventTelemetry(ev.Name)
	event.Timestamp = ev.Sent

	event.Properties["ID"] = ev.ID
	event.Properties["Type"] = "Outbound event"
	event.Properties["Status"] = ev.Status.String()

	event.Properties["Microservice"] = ev.Microservice
	event.Properties["Pod"] = ev.PodName

	event.Measurements["Attempts"] = float64(ev.Attempts)

	client.aiClient.Track(event)

	return nil
}
