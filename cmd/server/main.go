package main

import (
	"fmt"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	mqConnection := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(mqConnection)

	if err != nil {

	}

	defer conn.Close()

	fmt.Println("Connected to MQ")

	sigCh := make(chan os.Signal)

	signal.Notify(sigCh, os.Kill)
	signal.Notify(sigCh, os.Interrupt)

	select {
	case s := <-sigCh:
		fmt.Printf("\nSignal received: %s\n", s)
	}

	fmt.Println("Server stopped")
}
