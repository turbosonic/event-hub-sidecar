package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"event-delivery-sidecar/dto"
	"event-delivery-sidecar/factories"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
)

var (
	port            string
	eventReceiveURL string
	retryCount      int64
	retryInterval   int64
)

func main() {
	// set the default variables so we don't have to do it every time an event comes in
	getVariables()

	// Set up the connection to the MQ
	// This provides an interface to the message broker
	mqc := factories.MQClient()

	// ask the mq to listen for events, set this as a goroutine else we'll never move on
	go mqc.Listen(handleEvent)

	// now we can move on to the inbound API
	// create a new router
	router := mux.NewRouter()

	// TODO: add an endpoint for sending events to the broker

	// finally listen and serve the API
	log.Fatal(http.ListenAndServe(":"+port, router))
}

func getVariables() {
	// attempt to load environment variables from file
	err := godotenv.Load()
	if err != nil {
		log.Print("[x] using system environment variables")
	} else {
		log.Print("[x] using .env environment variables")
	}

	// is there a port are we using for the sidecar API? else default to 8989
	port = os.Getenv("PORT")
	if port == "" {
		port = "8989"
	}
	log.Print("[x] listening for http on localhost port " + port)

	// is there a specific path to send events on? else default to locahost:8080/event
	eventReceiveURL = os.Getenv("EVENT_RECEIEVE_URL")
	if eventReceiveURL == "" {
		eventReceiveURL = "http://localhost:8080/event"
	}
	log.Print("[x] received events will be posted to " + eventReceiveURL)

	// how many times do we try to send the event? default to 3
	retryCountStr := os.Getenv("EVENT_RECEIEVE_RETRY_COUNT")
	retryCount, err = strconv.ParseInt(retryCountStr, 10, 16)
	if err != nil {
		retryCount = 3
	}
	log.Print("[x] received events will attempted to sent " + strconv.FormatInt(retryCount, 10) + " time(s)")

	// how long do we wait to try again in seconds? default to 5
	retryIntervalStr := os.Getenv("EVENT_RECEIEVE_RETRY_INTERVAL")
	retryInterval, err = strconv.ParseInt(retryIntervalStr, 10, 16)
	if err != nil {
		retryInterval = 5
	}
	log.Print("[x] retries will be send after " + strconv.FormatInt(retryInterval, 10) + " second(s)")
}

func handleEvent(event dto.Event) dto.HandledEventStatus {

	// marshal the event
	payload, err := json.Marshal(&event)
	if err != nil {
		log.Printf("[!] Could not marshal event: ", err)

		// reject the event
		return dto.Rejected
	}

	var resp *http.Response

	// loop through the retries
	for i := int64(0); i < retryCount; i++ {
		resp, err = http.Post(eventReceiveURL, "application/json", bytes.NewBuffer(payload))
		if err != nil {
			time.Sleep(time.Duration(int64(time.Millisecond) * retryInterval))
		} else {
			i = retryCount
		}
	}

	// the service isn't available, release the event back into the queue
	if err != nil {
		log.Printf("[!] Could not send event to microservice: ", err)
		return dto.Released
	}

	// the event caused the service to error, release the event back into the queue
	if resp.StatusCode > 500 {
		log.Printf("[!] Recieved a "+strconv.FormatInt(int64(resp.StatusCode), 10)+" from the service : ", err)
		return dto.Released
	}

	// the event is not valid, reject the event
	if resp.StatusCode > 400 {
		log.Printf("[!] Recieved a "+strconv.FormatInt(int64(resp.StatusCode), 10)+" from the service : ", err)
		return dto.Rejected
	}

	// if we got here then we're all ok so tell the broker we've accepted the event
	log.Printf("[x] Event recieved by service")
	return dto.Accepted
}
