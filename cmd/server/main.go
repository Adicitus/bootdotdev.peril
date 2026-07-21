package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/adicitus/bootdotdev.peril/internal/pubsub"
	"github.com/adicitus/bootdotdev.peril/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	mqConnection := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(mqConnection)

	if err != nil {
		fmt.Printf("Error occurred while connection to MQ: %s", err.Error)
		os.Exit(1)
	}

	defer conn.Close()

	fmt.Println("Connected to MQ")

	msgCh, err := conn.Channel()

	if err != nil {
		conn.Close()
		fmt.Printf("Failed to generate MQ message channel: %s", err.Error)
		os.Exit(1)
	}

	pubsub.PublishJSON(msgCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
		IsPaused: true,
	})

	sigCh := make(chan os.Signal)

	signal.Notify(sigCh, os.Kill)
	signal.Notify(sigCh, os.Interrupt)

	select {
	case s := <-sigCh:
		fmt.Printf("\nSignal received: %s\n", s)
	}

	fmt.Println("Server stopped")
}
