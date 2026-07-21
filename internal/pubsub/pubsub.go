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
