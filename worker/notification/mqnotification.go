package notification

import (
	"fmt"
	"log"
	"main/utils"
	"main/worker/domain"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const EXCHANGE string = "notify"

type MQNotifier struct {
	notifications *utils.MapSet[domain.Notification]
	conn          *amqp.Connection
	ch            *amqp.Channel
	// q             amqp.Queue
}

func rabbitMqUrl() (string, error) {
	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	username := os.Getenv("RABBITMQ_USERNAME")
	password := os.Getenv("RABBITMQ_PASSWORD")

	if rabbitmqURL == "" || username == "" || password == "" {
		return "", fmt.Errorf("missing required environment variables")
	}

	connectionString := strings.Replace(rabbitmqURL, "amqps://", fmt.Sprintf("amqps://%s:%s@", username, password), 1)

	log.Printf("connection url: %s", connectionString)

	return connectionString, nil
}

func NewMQNotifier() (*MQNotifier, error) {
	url, err := rabbitMqUrl()
	if err != nil {
		return nil, err
	}

	conn, err := amqp.Dial(url)

	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		EXCHANGE, // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return nil, err
	}

	return &MQNotifier{
		notifications: utils.NewMapSet[domain.Notification](),
		conn:          conn,
		ch:            ch,
		// q:             q,
	}, nil

}

func (l *MQNotifier) Notify(workers ...string) error {
	for _, worker := range workers {
		err := l.ch.Publish(
			EXCHANGE,
			worker,
			false,
			false,
			amqp.Publishing{
				ContentType: "notification",
				Body:        []byte{},
			},
		)
		log.Printf("AMQP sent notification to %s on exchange %s", worker, EXCHANGE)

		if err != nil {
			return err
		}
	}
	return nil
}

func (l *MQNotifier) Close() error {
	defer l.conn.Close()
	defer l.ch.Close()

	return nil
}

type MQReceiver struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	q    amqp.Queue
}

func NewMQReceiver(workerId string) (*MQReceiver, error) {
	url, err := rabbitMqUrl()
	if err != nil {
		return nil, err
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		EXCHANGE, // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(q.Name, workerId, EXCHANGE, false, nil)
	if err != nil {
		return nil, err
	}

	log.Printf("AMQP bound queue %s to topic %s on exchange %s", q.Name, workerId, EXCHANGE)
	return &MQReceiver{
		conn: conn,
		ch:   ch,
		q:    q,
	}, nil

}

func (l *MQReceiver) Start(outputChannels ...chan<- time.Time) error {
	log.Printf("AMQP started receiver for queue %s", l.q.Name)
	msgs, err := l.ch.Consume(
		l.q.Name, // queue
		"",       // consumer
		true,     // auto ack
		false,    // exclusive
		false,    // no local
		false,    // no wait
		nil,      // args
	)
	if err != nil {
		return err
	}

	for d := range msgs {
		if d.ContentType == "notification" {
			t := time.Now()
			for _, channel := range outputChannels {
				select { // Non-blocking send
				case channel <- t:
				default:
				}
			}
		} else {
			log.Printf("Invalid content type for notification")
		}
	}

	return nil
}
