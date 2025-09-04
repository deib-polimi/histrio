package notification

import (
	"log"
	"main/utils"
	"main/worker/domain"
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

func NewMQNotifier(amqpUrl string) (*MQNotifier, error) {
	conn, err := amqp.Dial(amqpUrl)
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

func NewMQReceiver(amqpUrl string, workerId string) (*MQReceiver, error) {
	conn, err := amqp.Dial(amqpUrl)
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

	return &MQReceiver{
		conn: conn,
		ch:   ch,
		q:    q,
	}, nil

}

func (l *MQReceiver) Start(outputChannel chan<- time.Time) error {
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
			outputChannel <- time.Now()
		} else {
			log.Printf("Invalid content type for notification")
		}
	}

	return nil
}
