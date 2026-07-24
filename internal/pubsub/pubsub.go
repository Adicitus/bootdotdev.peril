package pubsub

import (
	"context"
	"encoding/json"

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
