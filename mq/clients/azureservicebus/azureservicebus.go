package azureservicebus

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	servicebus "github.com/Azure/azure-service-bus-go"

	"github.com/turbosonic/event-hub-sidecar/dto"
)

type MQClient struct {
	ns            *servicebus.Namespace
	outboundQueue *servicebus.Queue
	topic         *servicebus.Topic
}

func (mqc *MQClient) Listen(eventChan chan dto.Event, healthyChan chan bool) {

	mqc.connect(healthyChan)

	queueName := "queue/" + os.Getenv("MICROSERVICE_NAME")
	topicName := "topic/" + os.Getenv("MICROSERVICE_NAME")

	qm := mqc.ns.NewQueueManager()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(time.Second*5))

	existingQueues, err := qm.List(ctx)
	if err != nil {
		panic(err)
	}

	found := false
	for _, q := range existingQueues {
		if q.Name == queueName {
			found = true
		}
	}

	if !found {
		_, err := qm.Put(ctx, queueName)
		if err != nil {
			panic(err)
		}
	}

	cancel()

	ctx, cancel = context.WithTimeout(context.Background(), time.Duration(time.Second*5))

	q, err := mqc.ns.NewQueue(ctx, queueName)
	if err != nil {
		panic(err)
	}

	t, err := mqc.ns.NewTopic(ctx, topicName)
	if err != nil {
		panic(err)
	}

	s, err := t.NewSubscription(ctx, topicName)
	if err != nil {
		panic(err)
	}

	s.Receive(context.Background(),
		func(ctx context.Context, msg *servicebus.Message) servicebus.DispositionAction {
			log.Print("topic received")
			var e = dto.Event{}
			if err := json.Unmarshal(msg.Data, &e); err != nil {
				log.Print("Could not unmarshal event: ", err)
				return msg.Abandon()
			}

			handledChan := make(chan dto.HandledEventStatus)
			e.HandledStatus = &handledChan

			eventChan <- e

			switch <-handledChan {
			case dto.Accepted:
				return msg.Complete()
			case dto.Rejected:
				return msg.Abandon()
			default:
				return msg.Abandon()
			}
		})

	q.Receive(context.Background(),
		func(ctx context.Context, msg *servicebus.Message) servicebus.DispositionAction {
			var e = dto.Event{}
			if err := json.Unmarshal(msg.Data, &e); err != nil {
				log.Print("Could not unmarshal event: ", err)
				return msg.Abandon()
			}

			handledChan := make(chan dto.HandledEventStatus)
			e.HandledStatus = &handledChan

			eventChan <- e

			switch <-handledChan {
			case dto.Accepted:
				return msg.Complete()
			case dto.Rejected:
				return msg.Abandon()
			default:
				return msg.Abandon()
			}

		})

}

func (mqc *MQClient) Send(event *dto.Event) error {

	// Send message
	eventBytes, err := event.ToByteArray()
	if err != nil {
		return err
	}

	return mqc.outboundQueue.Send(context.Background(), servicebus.NewMessage(eventBytes))
}

func New() *MQClient {
	mqc := MQClient{}
	return &mqc
}

func (mqc *MQClient) connect(healthy chan bool) {

	healthy <- false

	queueName := os.Getenv("INGRESS_QUEUE_NAME")
	if queueName == "" {
		queueName = "event-hub"
	}

	connStr := os.Getenv("AZURE_SERVICEBUS_CONNECTION_STRING")
	ns, err := servicebus.NewNamespace(servicebus.NamespaceWithConnectionString(connStr))
	if err != nil {
		panic(err)
	}
	mqc.ns = ns

	mqc.outboundQueue, err = mqc.ns.NewQueue(context.Background(), queueName)
	if err != nil {
		panic(err)
	}

	log.Println("[i] connected to Azure Service Bus")

	healthy <- true
}
