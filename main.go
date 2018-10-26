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
	"event-delivery-sidecar/mq"

	"github.com/gorilla/mux"
	"github.com/joho/godotenv"
)

var (
	port            string
	eventReceiveURL string
	retryCount      int64
	retryInterval   int64
	mqc             mq.Client
)

func main() {
	// set the default variables so we don't have to do it every time an event comes in
	getVariables()

	// Set up the connection to the MQ
	// This provides an interface to the message broker
	mqc = factories.MQClient()

	// ask the mq to listen for events, set this as a goroutine else we'll never move on
	go mqc.Listen(handleEvent)

	// now we can move on to the inbound API
	// create a new router
	router := mux.NewRouter()

	// add an endpoint for sending events to the broker
	router.HandleFunc("/events/{event_name}", sendEvent).Methods("POST")

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

	// check we have a microservice name, else exit
	if os.Getenv("MICROSERVICE_NAME") == "" {
		log.Fatal("[!!!] No MICROSERVICE_NAME prodived, cannot continue, exiting...")
	}

	// is there a port are we using for the sidecar API? else default to 8989
	port = os.Getenv("PORT")
	if port == "" {
		port = "8989"
	}
	log.Print("[x] events can be sent to  http://localhost:" + port + "/events/{event_name}")

	// is there a specific path to send events on? else default to locahost:8080/event
	eventReceiveURL = os.Getenv("EVENT_RECEIEVE_URL")
	if eventReceiveURL == "" {
		eventReceiveURL = "http://localhost:8080/events"
	}
	log.Print("[x] received events will be posted to " + eventReceiveURL)

	// how many times do we try to send the event? default to 3
	retryCountStr := os.Getenv("EVENT_RECEIEVE_RETRY_COUNT")
	retryCount, err = strconv.ParseInt(retryCountStr, 10, 16)
	if err != nil {
		retryCount = 3
	}
	log.Print("[x] received events will attempted to be sent " + strconv.FormatInt(retryCount, 10) + " time(s)")

	// how long do we wait to try again in seconds? default to 5
	retryIntervalStr := os.Getenv("EVENT_RECEIEVE_RETRY_INTERVAL")
	retryInterval, err = strconv.ParseInt(retryIntervalStr, 10, 16)
	if err != nil {
		retryInterval = 5
	}
	log.Print("[x] retries will be sent after " + strconv.FormatInt(retryInterval, 10) + " second(s)")
}

func handleEvent(event dto.Event) dto.HandledEventStatus {

	// marshal the event
	payload, err := json.Marshal(&event)
	if err != nil {
		log.Printf("[!] Could not marshal event: %+v", err)

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
		log.Printf("[!] Could not send event to microservice: %+v", err)
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

func sendEvent(w http.ResponseWriter, r *http.Request) {

	// get the name of the event from the url
	params := mux.Vars(r)
	eventName := params["event_name"]

	// get the requestID from the header of the request
	requestID := r.Header.Get("x-request-id")

	// get the payload from the request
	var payload interface{}
	err := json.NewDecoder(r.Body).Decode(&payload)

	if err != nil {
		log.Printf("[!] Could not decode event: %+v", err)
		w.WriteHeader(400)
		return
	}

	// create the event to be sent
	e := dto.Event{}
	e.Name = eventName
	e.Source = os.Getenv("MICROSERVICE_NAME")
	e.Timestamp = time.Now()
	e.Payload = payload
	e.RequestID = requestID

	// loop through the retries trying to send it
	for i := int64(0); i < retryCount; i++ {
		err = mqc.Send(&e)
		if err != nil {
			time.Sleep(time.Duration(int64(time.Millisecond) * retryInterval))
		} else {
			i = retryCount
		}
	}

	// if it errored then respond with a 500
	if err != nil {
		log.Printf("[!] Could not send event to microservice: %+v", err)
		w.WriteHeader(500)
		return
	}

	// if we made it here then the event was delivered, respond with a 201
	w.WriteHeader(201)
}
