package factories

import (
	"log"
	"os"

	"github.com/turbosonic/event-hub-sidecar/logging"
	"github.com/turbosonic/event-hub-sidecar/logging/clients/applicationinsights"
	"github.com/turbosonic/event-hub-sidecar/logging/clients/influxdb"
	"github.com/turbosonic/event-hub-sidecar/logging/clients/stdout"
)

func LogClient() logging.Client {
	lp := os.Getenv("LOGGING_PROVIDER")

	switch lp {
	case "influxdb":
		log.Print("[i] Logging to InfluxDB")
		return influxdb.New()
	case "applicationinsights":
		log.Print("[i] Logging to Application Insights")
		return applicationinsights.New()
	default:
		log.Print("[i] Logging to stdout")
		return stdout.New()
	}
}
