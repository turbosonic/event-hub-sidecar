package activemq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"pack.ag/amqp"
)

type client struct {
}

func New() {

}

func connect() {
	// connect to AMQP queue
	var AMQPclient *amqp.Client

	err := errors.New("Not connected")

	for err != nil {
		AMQPclient, err = amqp.Dial(os.Getenv("ACTIVEMQ_AMQP_URL"),
			amqp.ConnSASLPlain(os.Getenv("ACTIVE_MQ_USERNAME"), os.Getenv("ACTIVE_MQ_PASSWORD")),
		)
		if err != nil {
			log.Println("Could not connect to AMQP:", err, "Retrying in 5 seconds...")
			time.Sleep(5 * time.Second)
		}
	}

	AMQPsession, err := AMQPclient.NewSession()
	if err != nil {
		log.Println("Creating AMQP session:", err)
	}

	sender, err = AMQPsession.NewSender(
		amqp.LinkTargetAddress("/event-hub"),
	)
	if err != nil {
		log.Panic("Creating sender link:", err)
	}

	// Create a receiver
	queueName := os.Getenv("MICROSERVICE_NAME")

	receiver, err := AMQPsession.NewReceiver(
		amqp.LinkSourceAddress("/"+queueName),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatal("Creating receiver link:", err)
	}

	go func() {
		for {
			// Receive next message
			msg, err := receiver.Receive(context.Background())
			if err != nil {
				log.Print("Reading message from AMQP:", err)
				connect()
				break
			}

			var e = Event{}
			if err := json.Unmarshal(msg.GetData(), &e); err != nil {
				fmt.Printf("Could not unmarshal event: ", err)
			}

			err = sendEvent(e)
			if err != nil {
				msg.Release()
				log.Println("Could not send event: ", err)

				// cooldown period
				time.Sleep(time.Second)
			} else {
				msg.Accept()
				log.Println("Event sent")
			}
		}
	}()

	log.Println("[x] Connected to AMQP")

	// connect to MQTT
	connOpts := mqtt.NewClientOptions().AddBroker(os.Getenv("ACTIVEMQ_MQTT_URL")).SetCleanSession(true)
	connOpts.SetUsername(os.Getenv("ACTIVE_MQ_USERNAME"))
	connOpts.SetPassword(os.Getenv("ACTIVE_MQ_PASSWORD"))
	tlsConfig := &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert}
	connOpts.SetTLSConfig(tlsConfig)

	err = errors.New("Not connected")
	var client mqtt.Client

	for err != nil {
		err = nil
		client = mqtt.NewClient(connOpts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			err = token.Error()
			log.Println("Dialing MQTT server:", err, " - Retrying in 5 seconds")
			time.Sleep(5 * time.Second)
		}
	}

	log.Println("[x] Connected to MQTT")

	client.Subscribe(os.Getenv("MICROSERVICE_NAME"), byte(0), func(client mqtt.Client, m mqtt.Message) {
		var e = Event{}
		if err := json.Unmarshal(m.Payload(), &e); err != nil {
			fmt.Printf("Could not unmarshal event: ", err)
		}

		sendEvent(e)
	})
}
