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

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queuename, key string, handler func(T) (ack, requeue bool)) error {

	msgCh, _, err := RegisterQueue(conn, exchange, queuename, key)

	if err != nil {
		return err
	}

	subCh, err := msgCh.Consume(queuename, "", false, false, false, false, nil)

	go func() {
		for delivery := range subCh {

			fmt.Printf("Handling %s...\n", delivery.CorrelationId)

			var msg T
			err := json.Unmarshal(delivery.Body, &msg)

			if err != nil {
				fmt.Printf("Error occured reading msg: %s\n", err.Error())
				continue
			}

			ack, requeue := handler(msg)

			if ack {
				fmt.Printf("Ack %s\n", delivery.CorrelationId)
				delivery.Ack(false)
			} else {
				fmt.Printf("Nack %s\n", delivery.CorrelationId)
				delivery.Nack(false, requeue)
			}
		}
	}()

	return nil
}

func RegisterQueue(conn *amqp.Connection, exchange, queuename, key string) (*amqp.Channel, amqp.Queue, error) {
	msgCh, err := conn.Channel()

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := msgCh.QueueDeclare(queuename, false, true, true, false, nil)

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
