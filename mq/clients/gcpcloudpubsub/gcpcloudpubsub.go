package gcpcloudpubsub

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	uuid "github.com/satori/go.uuid"
	"github.com/turbosonic/event-hub-sidecar/dto"
	"github.com/turbosonic/event-hub-sidecar/variables"
)

type Client struct {
	gcpClient     *pubsub.Client
	terminating   bool
	topics        map[string]*pubsub.Topic
	subscriptions map[string]*pubsub.Subscription

	id string

	topicEvents []string
	queueEvents []string
}

func New() *Client {
	client := Client{
		terminating:   false,
		topics:        make(map[string]*pubsub.Topic),
		subscriptions: make(map[string]*pubsub.Subscription),
		id:            os.Getenv("MICROSERVICE_NAME") + "-" + uuid.NewV4().String()[0:7],
	}
	return &client
}

func (client *Client) Listen(queueChan chan dto.Event, topicChan chan dto.Event, healthyChan chan bool, vars *variables.Data) {
	client.connect(healthyChan)

	queueEventsString := os.Getenv("queue_events")
	if queueEventsString != "" {
		client.queueEvents = strings.Split(queueEventsString, ",")
	}

	topicEventsString := os.Getenv("topic_events")
	if topicEventsString != "" {
		client.topicEvents = strings.Split(topicEventsString, ",")
	}

	for _, topic := range client.queueEvents {
		sub, err := client.getSubscription(context.Background(), os.Getenv("MICROSERVICE_NAME"), topic)
		if err != nil {
			log.Print(err)
		}

		go func(s *pubsub.Subscription) {
			for {
				err := sub.Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
					var e = dto.Event{}

					if err := json.Unmarshal(m.Data, &e); err != nil {
						log.Print("Could not unmarshal event: ", err)
						m.Nack()
					}

					handledChan := make(chan dto.HandledEventStatus)
					defer close(handledChan)

					e.HandledStatus = &handledChan

					queueChan <- e

					switch <-handledChan {
					case dto.Accepted:
						m.Ack()
					case dto.Rejected:
						m.Nack()
					default:
						m.Nack()
					}
				})
				if err != nil {
					log.Print(err)
				}
			}
		}(sub)
	}

	for _, topic := range client.topicEvents {
		sub, err := client.getSubscription(context.Background(), client.id, topic)
		if err != nil {
			log.Print(err)
		}

		go func(s *pubsub.Subscription) {
			for {
				err := sub.Receive(context.Background(), func(ctx context.Context, m *pubsub.Message) {
					var e = dto.Event{}

					if err := json.Unmarshal(m.Data, &e); err != nil {
						log.Print("Could not unmarshal event: ", err)
						m.Nack()
					}

					handledChan := make(chan dto.HandledEventStatus)
					defer close(handledChan)

					e.HandledStatus = &handledChan

					queueChan <- e

					switch <-handledChan {
					case dto.Accepted:
						m.Ack()
					case dto.Rejected:
						m.Nack()
					default:
						m.Nack()
					}
				})
				if err != nil {
					log.Print(err)
				}
			}
		}(sub)
	}
}

func (client *Client) Send(event *dto.Event) error {
	ctx := context.Background()

	topic, err := client.getTopic(ctx, event.Name)

	if err != nil {
		return err
	}

	eventBytes, err := event.ToByteArray()
	if err != nil {
		return err
	}

	res := topic.Publish(ctx, &pubsub.Message{Data: eventBytes})
	_, err = res.Get(ctx)

	return err
}

func (client *Client) connect(healthy chan bool) {

	healthy <- false

	projectId := os.Getenv("PUBSUB_PROJECT_ID")
	if projectId == "" {
		log.Fatal("[!!!] GCP Cloud Pub/Sub used, but PUBSUB_PROJECT_ID not found")
	}

	ctx := context.Background()

	err := errors.New("Not connected")
	for err != nil {
		if client.terminating {
			return
		}
		client.gcpClient, err = pubsub.NewClient(ctx, projectId)
		if err != nil {
			log.Println("[!] Could not connect to Cloud Pub/Sub:", err, "Retrying in 5 seconds...")
			time.Sleep(5 * time.Second)
		}
	}

	log.Println("[✓] connected to GCP Cloud Pub/Sub")

	healthy <- true

}

func (client *Client) getTopic(ctx context.Context, topicName string) (*pubsub.Topic, error) {
	// do we have it in local memory?
	topic, found := client.topics[topicName]

	// if not
	if !found {
		// use the gcp api to see if it exists
		existingTopic := client.gcpClient.Topic(topicName)
		ok, err := existingTopic.Exists(ctx)
		if err != nil {
			return nil, err
		}
		// if not
		if !ok {
			// make a new one
			newTopic, err := client.gcpClient.CreateTopic(ctx, topicName)
			if err != nil {
				return nil, err
			}
			client.topics[topicName] = newTopic
			return newTopic, nil
		}

		// if it did find it, use the existing one
		client.topics[topicName] = existingTopic
		return existingTopic, nil
	}

	return topic, nil
}

func (client *Client) getSubscription(ctx context.Context, serviceName string, topicName string) (*pubsub.Subscription, error) {
	subscriptionName := topicName + "-" + serviceName
	// do we have it in local memory?
	subscription, found := client.subscriptions[subscriptionName]

	if !found {

		topic, err := client.getTopic(ctx, topicName)
		if err != nil {
			return nil, err
		}

		existingSubscription := client.gcpClient.Subscription(subscriptionName)
		ok, err := existingSubscription.Exists(ctx)
		// if not
		if !ok {
			// make a new one
			newSubscription, err := client.gcpClient.CreateSubscription(context.Background(), subscriptionName,
				pubsub.SubscriptionConfig{Topic: topic})

			if err != nil {
				return nil, err
			}
			client.subscriptions[subscriptionName] = newSubscription
			return newSubscription, nil
		}

		// if it did find it, use the existing one
		client.subscriptions[subscriptionName] = existingSubscription
		return existingSubscription, nil
	}
	return subscription, nil
}

func (client *Client) Terminate() error {
	client.terminating = true

	for _, topic := range client.topicEvents {
		sub := client.gcpClient.Subscription(topic + "-" + client.id)
		if err := sub.Delete(context.Background()); err != nil {
			log.Print("Could not delete subscription: ", err)
		}
	}

	client.gcpClient.Close()
	return nil
}
