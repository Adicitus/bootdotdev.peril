package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/adicitus/bootdotdev.peril/internal/gamelogic"
	"github.com/adicitus/bootdotdev.peril/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	mqConnection := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(mqConnection)

	if err != nil {
		fmt.Printf("Failed to connect wiht  MQ server: %s\n", err.Error())
		os.Exit(1)
	}

	defer conn.Close()

	username, err := gamelogic.ClientWelcome()

	if err != nil {
		fmt.Printf("Failed to display greeting: %s\n", err.Error())
		os.Exit(1)
	}

	_, _, err = pubsub.RegisterQueue(conn, "peril_direct", fmt.Sprintf("pause.%s", username), "pause")

	if err != nil {
		fmt.Printf("Failed to register a message queue: %s\n", err.Error())
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	signal.Notify(sigCh, syscall.SIGHUP)
	signal.Notify(sigCh, syscall.SIGINT)

	s := <-sigCh

	fmt.Printf("Signal received: %s\n", s)
	fmt.Println("Client stopped.")
}
