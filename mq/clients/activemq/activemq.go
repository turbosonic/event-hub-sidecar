package activemq

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"os"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"pack.ag/amqp"
)

type MQClient struct {
	ctx         context.Context
	AMQPclient  *amqp.Client
	AMQPsession *amqp.Session
	MQTTclient  MQTT.Client
	// ************
	// --> START HERE senders     map[string]*amqp.Sender
}

func (mqc MQClient) Listen(c chan []byte) {
	queueName := os.Getenv("INGRESS_QUEUE_NAME")

	if queueName == "" {
		queueName = "event-hub"
	}

	// Create a receiver
	receiver, err := mqc.AMQPsession.NewReceiver(
		amqp.LinkSourceAddress("/"+queueName),
		amqp.LinkCredit(10),
	)
	if err != nil {
		log.Fatal("Creating receiver link:", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(mqc.ctx, 1*time.Second)
		receiver.Close(ctx)
		cancel()
	}()

	for {
		// Receive next message
		msg, err := receiver.Receive(mqc.ctx)
		if err != nil {
			log.Print("Reading message from AMQP:", err)
			connect(&mqc)
			break
		}

		c <- msg.GetData()

		msg.Accept()
	}
}

func (mqc MQClient) SendToQueue(queueName string, event *[]byte) error {

	// Create a sender
	_, found := mqc.senders[queueName]
	if found == false {
		newsender, err := mqc.AMQPsession.NewSender(
			amqp.LinkTargetAddress("/" + queueName),
		)
		if err != nil {
			log.Println("Creating sender link:", err)
			return err
		}
		mqc.senders[queueName] = newsender
	}

	sender := mqc.senders[queueName]

	// Send message
	err := sender.Send(mqc.ctx, amqp.NewMessage(*event))
	if err != nil {
		log.Println("Sending message:", err)
	}
	return err
}

func (mqc MQClient) SendToTopic(topicName string, event *[]byte) error {
	tk := mqc.MQTTclient.Publish(topicName, byte(0), false, *event)
	return tk.Error()
}

func New() MQClient {
	mqc := MQClient{}
	mqc.senders = make(map[string]*amqp.Sender)

	connect(&mqc)

	return mqc
}

func connect(mqc *MQClient) {
	// Create client
	err := errors.New("Not connected")

	for err != nil {
		mqc.AMQPclient, err = amqp.Dial(os.Getenv("ACTIVEMQ_AMQP_URL"),
			amqp.ConnSASLPlain(os.Getenv("ACTIVE_MQ_USERNAME"), os.Getenv("ACTIVE_MQ_PASSWORD")),
		)
		if err != nil {
			log.Println("Could not connect to AMQP:", err, "Retrying in 5 seconds...")
		}
		time.Sleep(5 * time.Second)
	}

	log.Println("[x] Connected to AMQP")

	// Open a session
	mqc.AMQPsession, err = mqc.AMQPclient.NewSession()
	if err != nil {
		log.Println("Creating AMQP session:", err)
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
}
