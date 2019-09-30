package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

  "github.com/turbosonic/event-hub-sidecar/dto"
	"github.com/turbosonic/event-hub-sidecar/factories"
	"github.com/turbosonic/event-hub-sidecar/inboundevents"
	"github.com/turbosonic/event-hub-sidecar/logging"
	"github.com/turbosonic/event-hub-sidecar/mq"
	"github.com/turbosonic/event-hub-sidecar/variables"

	"github.com/gorilla/mux"
)

var (
	vars    variables.Data
	mqc     mq.Client
	lh      logging.Handler
	server  http.Server
	healthy = false
)

func main() {
	// uncomment below for profiling
	//defer profile.Start(profile.MemProfile).Stop()

	// set the default variables so we don't have to do it every time an event comes in
	vars = variables.GetVariables()

	// first of all we need to handle signal interupts
	// with Kubernetes scaling at will, it means that we could be asked to
	// terminate at any time and if we're handing any events at that time
	// we should wait for them to finish processing before exiting
	var terminate sync.WaitGroup
	terminate.Add(1)

	go func() {
		sigint := make(chan os.Signal, 1)

		// interrupt signal sent from terminal
		signal.Notify(sigint, os.Interrupt)
		// sigterm signal sent from kubernetes
		signal.Notify(sigint, syscall.SIGTERM)

		<-sigint
		log.Print("[i] kill signal received, waiting for outstanding events to be delivered...")

		mqc.Terminate()

		terminate.Done()
	}()

	// Set up the connection to the MQ
	// This provides an interface to the message broker
	mqc = factories.MQClient()

	// Setup logging client
	lc := factories.LogClient()
	lh = logging.New(lc)

	log.Print("")

	// ask the mq to listen for events, set this as a goroutine else we'll never move on
	// pass in a bool channel to recieve indicators of connections
	healthyChan := make(chan bool)
	queueChan := make(chan dto.Event)
	topicChan := make(chan dto.Event)
	errorChan := make(chan error)

	// wait for a message from the channel and update the health variable
	go func() {
		for {
			h, cont := <-healthyChan
			if !cont {
				break
			}
			healthy = h
		}
	}()

	go func() {
		for {
			<-errorChan
			if vars.CanaryEventInterval > 0 {
				log.Print("[!] Error during canary period, closing up")
				mqc.Terminate()
				terminate.Done()
			}
		}
	}()

	// check for the end of the canary period
	go func() {
		if vars.CanaryEventInterval > 0 {
			for {
				if time.Now().After(vars.CanaryEnd) {
					vars.CanaryEventInterval = 0
					log.Print("[i] Canary period complete, processing at full speed")
					break
				}
				time.Sleep(time.Second)
			}
		}
	}()

	// events via queues
	go func() {
		for {
			// wait for the event
			ev, cont := <-queueChan
			if !cont {
				break
			}

			// handle the event
			go inboundevents.HandleEvent(&ev, &vars, errorChan, &lh)

			// some logic here to slow things down if required for canary
			time.Sleep(time.Duration(int64(time.Second) * vars.CanaryEventInterval))
		}
	}()

	// events via topics
	go func() {
		for {
			// wait for the event
			ev, cont := <-topicChan
			if !cont {
				break
			}

			// nothing clever here, just process it
			go inboundevents.HandleEvent(&ev, &vars, errorChan, &lh)
		}

	}()

	mqc.Listen(queueChan, topicChan, healthyChan, &vars)

	// now we can move on to the inbound API
	// create a new router
	router := mux.NewRouter()

	// add an endpoint for sending events to the broker
	router.HandleFunc("/events/{event_name}", sendEvent).Methods("POST")

	// add a health endpoint
	router.HandleFunc("/health", handleHeathRequest).Methods("GET")

	srv := &http.Server{Addr: ":" + vars.Port, Handler: router}

	// finally listen and serve the API
	go srv.ListenAndServe()

	// wait for a terminate signal and close the server
	terminate.Wait()
	srv.Close()
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
	e.Source = vars.MicroserviceName
	e.Timestamp = time.Now()
	e.Payload = payload
	e.RequestID = requestID

	var attempts int64 = 0

	// loop through the retries trying to send it
	for i := int64(0); i < vars.RetryCount; i++ {
		err = mqc.Send(&e)
		if err != nil {
			log.Print("[!] event failed attempt ", i+1)
			time.Sleep(time.Duration(int64(time.Second) * vars.RetryInterval))
		} else {
			i = vars.RetryCount
		}
		attempts++
	}

	// if it errored then respond with a 500
	if err != nil {
		log.Printf("[!] Could not send event to microservice: %+v", err)
		w.WriteHeader(500)
		lh.LogOutboundEvent(&e, dto.Failed, &vars, attempts)
		return
	}

	// if we made it here then the event was delivered, respond with a 201
	w.WriteHeader(201)

	lh.LogOutboundEvent(&e, dto.Succeeded, &vars, attempts)
}

func handleHeathRequest(w http.ResponseWriter, r *http.Request) {

	// if healthy is false then return a 400, else 200
	if !healthy {
		w.WriteHeader(400)
		return
	}
	w.WriteHeader(200)
}
