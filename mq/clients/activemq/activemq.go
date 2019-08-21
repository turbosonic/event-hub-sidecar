package activemq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"pack.ag/amqp"

	"github.com/turbosonic/event-hub-sidecar/dto"
	"github.com/turbosonic/event-hub-sidecar/variables"
)

type MQClient struct {
	ctx         context.Context
	AMQPclient  *amqp.Client
	AMQPsession *amqp.Session
	MQTTclient  MQTT.Client
	sender      *amqp.Sender
	wg          sync.WaitGroup
	terminating bool

	queueChan   chan dto.Event
	topicChan   chan dto.Event
	healthyChan chan bool
}

func (mqc *MQClient) Terminate() error {
	if !mqc.terminating {

		mqc.healthyChan <- false
		mqc.terminating = true

		mqc.wg.Wait()

		mqc.AMQPsession.Close(context.Background())
		mqc.MQTTclient.Unsubscribe(os.Getenv("MICROSERVICE_NAME"))

		close(mqc.queueChan)
		close(mqc.topicChan)
		close(mqc.healthyChan)
	}

	return nil
}

func (mqc *MQClient) Listen(queueChan chan dto.Event, topicChan chan dto.Event, healthyChan chan bool, vars *variables.Data) {

	mqc.queueChan = queueChan
	mqc.topicChan = topicChan
	mqc.healthyChan = healthyChan

	mqc.connect(mqc.healthyChan)

	log.Println("[i] waiting for events...")

	queueName := os.Getenv("MICROSERVICE_NAME")

	// Create a receiver
	receiver, err := mqc.AMQPsession.NewReceiver(
		amqp.LinkSourceAddress("/"+queueName),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatal("Creating receiver link:", err)
	}

	// Create a semaphore channel which will fill up to the max concurrency (and thus pause processing queue events) and empty as the service responds
	semaphore := make(chan bool, vars.EventConcurrency)

	go func() {
		for {
			semaphore <- true

			// break the loop if we're terminating
			if mqc.terminating {
				break
			}

			// Receive next message
			msg, err := receiver.Receive(context.Background())

			if err != nil {
				log.Print("[!] disconnected from AMQP: ", err)
				if !mqc.terminating {
					mqc.connect(mqc.healthyChan)
				}
				break
			}
			go func(sem chan bool, wg *sync.WaitGroup) {
				defer func() {
					wg.Done()
					<-sem
				}()

				wg.Add(1)
				var e = dto.Event{}
				if err := json.Unmarshal(msg.GetData(), &e); err != nil {
					log.Print("Could not unmarshal event: ", err)
					msg.Release()
				}

				handledChan := make(chan dto.HandledEventStatus)
				e.HandledStatus = &handledChan

				mqc.queueChan <- e

				switch <-handledChan {
				case dto.Accepted:
					msg.Accept()
				case dto.Rejected:
					msg.Release()
				default:
					msg.Release()
				}

				close(handledChan)

			}(semaphore, &mqc.wg)

		}
		receiver.Close(context.Background())
	}()

	mqc.MQTTclient.Subscribe(os.Getenv("MICROSERVICE_NAME"), byte(0), func(client MQTT.Client, m MQTT.Message) {
		mqc.wg.Add(1)
		var e = dto.Event{}
		if err := json.Unmarshal(m.Payload(), &e); err != nil {
			fmt.Printf("Could not unmarshal event: ", err)
		}

		handledChan := make(chan dto.HandledEventStatus)
		e.HandledStatus = &handledChan

		mqc.topicChan <- e

		<-handledChan
		close(handledChan)
		mqc.wg.Done()
	})
}

func (mqc *MQClient) Send(event *dto.Event) error {

	// Send message
	eventBytes, err := event.ToByteArray()
	if err != nil {
		return err
	}

	err = mqc.sender.Send(mqc.ctx, amqp.NewMessage(eventBytes))
	if err != nil {
		return err
	}

	return nil
}

func New() *MQClient {
	mqc := MQClient{}
	mqc.terminating = false
	return &mqc
}

func (mqc *MQClient) connect(healthy chan bool) {

	healthy <- false

	queueName := os.Getenv("INGRESS_QUEUE_NAME")
	if queueName == "" {
		queueName = "event-hub"
	}

	// Create client
	err := errors.New("Not connected")

	for err != nil {
		mqc.AMQPclient, err = amqp.Dial(os.Getenv("ACTIVEMQ_AMQP_URL"),
			amqp.ConnSASLPlain(os.Getenv("ACTIVE_MQ_USERNAME"), os.Getenv("ACTIVE_MQ_PASSWORD")),
		)
		if err != nil {
			log.Println("[!] Could not connect to AMQP:", err, "Retrying in 5 seconds...")
			time.Sleep(5 * time.Second)
		}
	}

	// Open a session
	mqc.AMQPsession, err = mqc.AMQPclient.NewSession()
	if err != nil {
		log.Println("[!] Failed when creating AMQP session:", err)
	}

	// create a sender
	mqc.sender, err = mqc.AMQPsession.NewSender(
		amqp.LinkTargetAddress("/" + queueName),
	)
	if err != nil {
		log.Println("[!] Failed when creating AMQP sender link:", err)
		return
	}

	mqc.ctx = context.Background()

	connOpts := MQTT.NewClientOptions().AddBroker(os.Getenv("ACTIVEMQ_MQTT_URL")).SetCleanSession(true)
	connOpts.SetUsername(os.Getenv("ACTIVE_MQ_USERNAME"))
	connOpts.SetPassword(os.Getenv("ACTIVE_MQ_PASSWORD"))
	tlsConfig := &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert}
	connOpts.SetTLSConfig(tlsConfig)

	mqc.MQTTclient = MQTT.NewClient(connOpts)
	if token := mqc.MQTTclient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	log.Println("[✓] connected to ActiveMQ via AMQP and MQTT")

	healthy <- true
}
