package variables

import (
	"log"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

type Data struct {
	MicroserviceName    string
	Port                string
	EventReceiveURL     string
	RetryCount          int64
	RetryInterval       int64
	CanaryEventInterval int64
	EventConcurrency    int64
	CanaryEnd           time.Time
}

func GetVariables() Data {

	data := Data{}

	// attempt to load environment variables from file
	err := godotenv.Load()
	if err != nil {
		log.Print("[i] using system environment variables")
	} else {
		log.Print("[i] using .env environment variables")
	}

	// check we have a microservice name, else exit
	if os.Getenv("MICROSERVICE_NAME") == "" {
		log.Fatal("[!!!] No MICROSERVICE_NAME prodived, cannot continue, exiting...")
	}
	data.MicroserviceName = os.Getenv("MICROSERVICE_NAME")
	log.Print("[i] listening to topic and queue '" + data.MicroserviceName + "'")

	// is there a port are we using for the sidecar API? else default to 8989
	data.Port = os.Getenv("PORT")
	if data.Port == "" {
		data.Port = "8989"
	}
	log.Print("[i] events can be sent to http://localhost:" + data.Port + "/events/{event_name}")

	// is there a specific path to send events on? else default to locahost:8080/event
	data.EventReceiveURL = os.Getenv("EVENT_RECEIEVE_URL")
	if data.EventReceiveURL == "" {
		data.EventReceiveURL = "http://localhost:8080/events/"
	}
	log.Print("[i] received events will be posted to " + data.EventReceiveURL + "{event_name}")

	// how many times do we try to send the event? default to 3
	retryCountStr := os.Getenv("EVENT_RECEIEVE_RETRY_COUNT")
	data.RetryCount, err = strconv.ParseInt(retryCountStr, 10, 16)
	if err != nil {
		data.RetryCount = 3
	}
	log.Print("[i] received events will attempted to be sent " + strconv.FormatInt(data.RetryCount, 10) + " time(s)")

	// how long do we wait to try again in seconds? default to 5
	retryIntervalStr := os.Getenv("EVENT_RECEIEVE_RETRY_INTERVAL")
	data.RetryInterval, err = strconv.ParseInt(retryIntervalStr, 10, 16)
	if err != nil {
		data.RetryInterval = 5
	}
	log.Print("[i] retries will be sent after " + strconv.FormatInt(data.RetryInterval, 10) + " second(s)")

	// how many events can be sent concurrently
	eventConcurrencyStr := os.Getenv("EVENT_MAX_CONCURRENCY")
	data.EventConcurrency, err = strconv.ParseInt(eventConcurrencyStr, 10, 16)
	if err != nil {
		data.EventConcurrency = 1
	}
	log.Print("[i] maximum events that can be sent concurrently is " + strconv.FormatInt(data.EventConcurrency, 10))

	// Canary mode how long do we wait between events in seconds? default to 0
	canaryEventIntervalStr := os.Getenv("CANARY_EVENT_INTERVAL")
	data.CanaryEventInterval, err = strconv.ParseInt(canaryEventIntervalStr, 10, 16)
	if err != nil {
		data.CanaryEventInterval = 0
	} else {
		log.Print("[i] CANARY MODE ACTIVE: events will be proccessed every " + strconv.FormatInt(data.CanaryEventInterval, 10) + " second(s)")
	}

	// Canary mode how long does it last in seconds? default to 0
	canaryPeriodStr := os.Getenv("CANARY_PERIOD")
	canaryPeriod, err := strconv.ParseInt(canaryPeriodStr, 10, 16)
	if err != nil {
		canaryPeriod = 0
		log.Print("[i] canary mode is not active")
	} else {
		log.Print("[i] canary mode will be active for " + strconv.FormatInt(canaryPeriod, 10) + " second(s)")
	}

	data.CanaryEnd = time.Now().Add(time.Duration(int64(time.Second) * canaryPeriod))

	return data
}
