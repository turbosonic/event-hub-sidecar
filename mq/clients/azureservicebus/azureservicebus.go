// Hello! This code will send and receive messages from Azure Service Bus
// This is a bit of a departure from previous iterations of the sidecar
// You see, it requires no event hub, the sidecar sets up topics and subscriptions
// which means the event distribution happens in an identical way, just with less compute requirements
// this is WAY better, because we now just have stateless services
// Each service has a sidecar to choreograph its events
// I've already written too many comments, I'll have to document how it works elsewhere

package azureservicebus

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"
	uuid "github.com/satori/go.uuid"

	"github.com/turbosonic/event-hub-sidecar/dto"
	"github.com/turbosonic/event-hub-sidecar/variables"
)

type MQClient struct {
	ns     *servicebus.Namespace
	topics map[string]*servicebus.Topic

	queueChan   chan dto.Event
	topicChan   chan dto.Event
	healthyChan chan bool

	id string

	topicEvents []string
	queueEvents []string
}

func New() *MQClient {
	mqc := MQClient{}
	mqc.topics = make(map[string]*servicebus.Topic)
	mqc.id = os.Getenv("MICROSERVICE_NAME") + "-" + uuid.NewV4().String()[0:7]
	return &mqc
}

func (mqc *MQClient) Listen(queueChan chan dto.Event, topicChan chan dto.Event, healthyChan chan bool, vars *variables.Data) {
	mqc.connect(healthyChan)

	// alright, so here we need to know which events that the service wants to listen to, there are two types:
	// Type I) an event which is only delivered to one instance of the service, probably a worker
	// 		   e.g. "A user signed up, I must send an email to them"
	// Type II) an event which is delivered to all instances of the service, probably a cache invalidation
	//		   e.g. "A user signed up, I must re-create my in memory cache of users"

	// we will find the event subscriptions in the variables

	queueEventsString := os.Getenv("queue_events")
	if queueEventsString != "" {
		mqc.queueEvents = strings.Split(queueEventsString, ",")
	}

	topicEventsString := os.Getenv("topic_events")
	if topicEventsString != "" {
		mqc.topicEvents = strings.Split(topicEventsString, ",")
	}

	// for the Type I) "queue" events each instance shares a subscription with a topic
	// we call that subscription the same name as the microservice
	// HANG ON A MINUTE; you see the thing is, we could be listening out for events that don't yet exist
	// ergo don't have a topic on Azure Service Bus
	// vis-à-vis subscribing to a non-existant topic will fail
	// so we need to make the topic if it doesn't already exist

	for _, topic := range mqc.queueEvents {

		mqc.ensureTopic(topic)

		t, err := mqc.ns.NewTopic(topic)
		if err != nil {
			log.Panic(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*5))
		defer cancel()

		sm, err := mqc.ns.NewSubscriptionManager(topic)
		if err != nil {
			log.Panic(err)
		}
		sm.Put(ctx, os.Getenv("MICROSERVICE_NAME"))

		// subscribe with microservice name
		s, err := t.NewSubscription(os.Getenv("MICROSERVICE_NAME"))
		if err != nil {
			log.Panic(err)
		}

		go func() {
			for {
				err := s.Receive(context.Background(),
					servicebus.HandlerFunc(func(ctx context.Context, msg *servicebus.Message) error {
						var e = dto.Event{}
						if err := json.Unmarshal(msg.Data, &e); err != nil {
							log.Print("Could not unmarshal event: ", err)
							return msg.Abandon(ctx)
						}

						handledChan := make(chan dto.HandledEventStatus)
						defer close(handledChan)

						e.HandledStatus = &handledChan

						queueChan <- e

						switch <-handledChan {
						case dto.Accepted:
							return msg.Complete(ctx)
						case dto.Rejected:
							return msg.Abandon(ctx)
						default:
							return msg.Abandon(ctx)
						}
					}))
				log.Println(err)
			}
		}()
	}

	// for the Type II "topic" events, each instance has it's own unique subscription with a top
	// thus we call that subscription the name of the microservice plus a hash generated at runtime
	// we must remove this subscription when terminating, checkout out the Terninate() function, you'll see it
	for _, topic := range mqc.topicEvents {
		mqc.ensureTopic(topic)

		// subscribe with microservice name + uuid
		t, err := mqc.ns.NewTopic(topic)
		if err != nil {
			log.Panic(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*60))
		defer cancel()

		sm, err := mqc.ns.NewSubscriptionManager(topic)
		if err != nil {
			log.Panic(err)
		}
		sm.Put(ctx, mqc.id)

		// subscribe with microservice name
		s, err := t.NewSubscription(mqc.id)
		if err != nil {
			log.Panic(err)
		}

		go func() {
			for {
				err := s.Receive(context.Background(),
					servicebus.HandlerFunc(func(ctx context.Context, msg *servicebus.Message) error {
						var e = dto.Event{}
						if err := json.Unmarshal(msg.Data, &e); err != nil {
							log.Print("Could not unmarshal event: ", err)
							return msg.Abandon(ctx)
						}

						handledChan := make(chan dto.HandledEventStatus)
						defer close(handledChan)

						e.HandledStatus = &handledChan

						topicChan <- e

						switch <-handledChan {
						case dto.Accepted:
							return msg.Complete(ctx)
						case dto.Rejected:
							return msg.Abandon(ctx)
						default:
							return msg.Abandon(ctx)
						}
					}))
				log.Println(err)
			}
		}()
	}

}

func (mqc *MQClient) Send(event *dto.Event) error {
	// sending events is a little different with Azure Service Bus as well
	// we need to put each event into a topic based on its name
	// we don't know what type (name) of events might come from the service
	// thus we must be prepared to create a new topic for each new event that comes in if it doesn't already exist
	// but we don't want to do that all the time, else sending events will be slow
	// so we need to remember the topics that we know already exist so we can just send something real quick

	// do we have the topic already?
	topic, found := mqc.topics[event.Name]

	// no, ok let's make it
	if !found {
		// now this could fail, but we're going to swallow the error (WTF?)
		// WTF cont... this is because another instance may have created the topic since we checked
		// and the fail could be a Azure saying that the topic already exists
		// if we continue and this hasn't been the case, creating the connection will fail
		mqc.ensureTopic(event.Name)

		// create a connection
		t, err := mqc.ns.NewTopic(event.Name)
		if err != nil {
			return err
		}

		// add it to the map
		mqc.topics[event.Name] = t

		// store it as topic
		topic = t
	}

	eventBytes, err := event.ToByteArray()
	if err != nil {
		return err
	}

	return topic.Send(context.Background(), servicebus.NewMessage(eventBytes))
}

func (mqc *MQClient) connect(healthy chan bool) {
	// if we're connecting, then we're not healthy, make it so
	healthy <- false

	// here we setup a subscription to Azure Service Bus using provided credentials
	// this will also be called if, for any reason, we become disconnected from Azure
	// if we can't connect for any reason, we need to panic out, we literally can't do anything if we can't connect
	connStr := os.Getenv("AZURE_SERVICEBUS_CONNECTION_STRING")
	if connStr == "" {
		log.Panic("AZURE_SERVICEBUS_CONNECTION_STRING not found in environment")
	}
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(connStr))
	if err != nil {
		log.Panic(err)
	}

	mqc.ns = ns

	// when that's done, we're healthy
	log.Println("[i] connected to Azure Service Bus as " + mqc.id)
	healthy <- true
}

// Deletes the individual topic
func (mqc *MQClient) Terminate() error {
	// here we need to close everything down
	// disconnect from the subscriptions we have created
	// delete the subscription which is unique to this instance of the service

	// close all the outbound channels,
	for _, topic := range mqc.topicEvents {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*5))
		defer cancel()

		sm, err := mqc.ns.NewSubscriptionManager(topic)
		if err != nil {
			log.Panic(err)
		}
		sm.Delete(ctx, mqc.id)
	}

	// no errors? Hurrah!
	return nil
}

// ensureTopic checks whether a topic exists on the Service Bus and creates it if not
func (mqc *MQClient) ensureTopic(topicName string) error {
	// does it exist on Azure?
	tm := mqc.ns.NewTopicManager()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*5))
	defer cancel()

	existingTopics, err := tm.List(ctx)
	if err != nil {
		return err
	}
	azFound := false
	for _, t := range existingTopics {
		if t.Name == topicName {
			azFound = true
		}
	}

	// it doesn't exist on azure, so we need to create a new topic
	if !azFound {
		_, err := tm.Put(ctx, topicName)
		if err != nil {
			return err
		}
	}

	return nil
}
