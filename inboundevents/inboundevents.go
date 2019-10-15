package inboundevents

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/turbosonic/event-hub-sidecar/logging"

	"github.com/turbosonic/event-hub-sidecar/variables"

	"github.com/turbosonic/event-hub-sidecar/dto"
)

func HandleEvent(event *dto.Event, vars *variables.Data, errorChan chan error, lh *logging.Handler) {

	startTime := time.Now()

	// marshal the event
	payload, err := json.Marshal(&event.Payload)
	if err != nil {
		log.Printf("[!] Could not marshal event: %+v", err)

		// reject the event
		*event.HandledStatus <- dto.Rejected

		// wait for the channel to close (event has been rejected)
		<-*event.HandledStatus

		errorChan <- err

		lh.LogInboundEvent(event, dto.Rejected, vars, time.Since(startTime).Nanoseconds(), 0)
		return
	}

	client := &http.Client{}

	req, _ := http.NewRequest("POST", vars.EventReceiveURL+event.Name, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("x-request-id", event.RequestID)
	req.Header.Set("x-source", event.Source)

	var resp *http.Response

	var attempts int64 = 0

	// loop through the retries
	for i := int64(0); i < vars.RetryCount; i++ {
		resp, err = client.Do(req)
		if err != nil && i != vars.RetryCount-1 {
			time.Sleep(time.Duration(int64(time.Second) * vars.RetryInterval))
		} else {
			i = vars.RetryCount
		}
		attempts++
	}

	// the service isn't available, release the event back into the queue
	if err != nil {
		log.Printf("[!] Could not send event to microservice: %+v", err)
		*event.HandledStatus <- dto.Released

		// wait for the channel to close (event has been released)
		<-*event.HandledStatus

		errorChan <- err
		lh.LogInboundEvent(event, dto.Released, vars, time.Since(startTime).Nanoseconds(), attempts)
		return
	}

	// the event caused the service to error, release the event back into the queue
	if resp.StatusCode > 500 {
		log.Printf("[!] Recieved a "+strconv.FormatInt(int64(resp.StatusCode), 10)+" from the service : ", err)
		*event.HandledStatus <- dto.Released

		// wait for the channel to close (event has been released)
		<-*event.HandledStatus

		errorChan <- err
		lh.LogInboundEvent(event, dto.Released, vars, time.Since(startTime).Nanoseconds(), attempts)
		return
	}

	// the event is not valid, reject the event
	if resp.StatusCode > 400 {
		log.Printf("[!] Recieved a "+strconv.FormatInt(int64(resp.StatusCode), 10)+" from the service : ", err)
		*event.HandledStatus <- dto.Rejected

		// wait for the channel to close (event has been rejected)
		<-*event.HandledStatus

		errorChan <- err
		lh.LogInboundEvent(event, dto.Rejected, vars, time.Since(startTime).Nanoseconds(), attempts)
		return
	}

	// if we got here then we're all ok so tell the broker we've accepted the event
	log.Printf("[✓] event recieved by service")
	*event.HandledStatus <- dto.Accepted

	// wait for the channel to close (event has been accepted)
	<-*event.HandledStatus

	lh.LogInboundEvent(event, dto.Accepted, vars, time.Since(startTime).Nanoseconds(), attempts)

	return
}
