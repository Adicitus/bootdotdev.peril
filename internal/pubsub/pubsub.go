package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)

	if err != nil {
		return err
	}

	msg := amqp.Publishing{
		Headers: amqp.Table{
			"Content-Type": "application/json;charset=utf-8",
		},
		Body: data,
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)

	if err != nil {
		return err
	}

	return nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queuename, key string, durable, exclusive bool, handler func(T) (ack, requeue bool)) error {

	msgCh, _, err := RegisterQueue(conn, exchange, queuename, key, durable, exclusive)

	if err != nil {
		return err
	}

	subCh, err := msgCh.Consume(queuename, "", false, false, false, false, nil)

	go func() {
		for delivery := range subCh {

			var msg T
			err := json.Unmarshal(delivery.Body, &msg)

			if err != nil {
				fmt.Printf("Error occured reading msg: %s\n", err.Error())
				continue
			}

			ack, requeue := handler(msg)

			if ack {
				delivery.Ack(false)
			} else {
				delivery.Nack(false, requeue)
			}
		}
	}()

	return nil
}

func RegisterQueue(conn *amqp.Connection, exchange, queuename, key string, durable, exclusive bool) (*amqp.Channel, amqp.Queue, error) {
	msgCh, err := conn.Channel()

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := msgCh.QueueDeclare(queuename, durable, true, exclusive, false, amqp.Table{
		"x-dead-letter-exchange": "peril_dlx",
	})

	if err != nil {
		msgCh.Close()
		return nil, amqp.Queue{}, err
	}

	err = msgCh.QueueBind(queuename, key, exchange, false, nil)

	if err != nil {
		msgCh.Close()
		return nil, amqp.Queue{}, err
	}

	return msgCh, queue, nil
}
