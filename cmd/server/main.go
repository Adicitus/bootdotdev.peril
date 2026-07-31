package main

import (
	"fmt"
	"os"
	"time"

	"github.com/adicitus/bootdotdev.peril/internal/gamelogic"
	"github.com/adicitus/bootdotdev.peril/internal/pubsub"
	"github.com/adicitus/bootdotdev.peril/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	mqConnection := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(mqConnection)

	if err != nil {
		fmt.Printf("Error occurred while connection to MQ: %s\n", err.Error())
		os.Exit(1)
	}

	defer conn.Close()

	fmt.Println("Connected to MQ")

	msgCh, err := conn.Channel()

	if err != nil {
		conn.Close()
		fmt.Printf("Failed to generate MQ message channel: %s\n", err.Error())
		os.Exit(1)
	}

	gamelogic.PrintServerHelp()
GameLoop:
	for {
		input := gamelogic.GetInput()

		if len(input) == 0 {
			time.Sleep(time.Millisecond * 5)
			continue
		}

		switch input[0] {
		case "pause":
			fmt.Println("Sending pause message")
			pubsub.PublishJSON(msgCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
		case "resume":
			fmt.Println("Sending resume message")
			pubsub.PublishJSON(msgCh, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
		case "quit":
			fmt.Println("Server shutting down...")
			break GameLoop
		default:
			fmt.Printf("Unrecognized command: %s\n", input[0])
		}
	}
	fmt.Println("Server stopped")
}
