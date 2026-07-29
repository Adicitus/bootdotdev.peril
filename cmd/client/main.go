package main

import (
	"fmt"
	"os"
	"time"

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

	state := gamelogic.NewGameState(username)

GameLoop:
	for {
		input := gamelogic.GetInput()

		if len(input) == 0 {
			time.Sleep(time.Millisecond * 5)
			continue
		}

		switch input[0] {
		case "spawn":
			err = state.CommandSpawn(input)
			if err != nil {
				fmt.Printf("An error occurred: %s\n", err.Error())
			}

		case "move":
			move, err := state.CommandMove(input)

			if err != nil {
				fmt.Printf("An error occurred: %s\n", err.Error())
				continue
			}

			fmt.Printf("Move %d to %s", len(move.Units), move.ToLocation)

		case "status":
			state.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet")

		case "quit":
			gamelogic.PrintQuit()
			break GameLoop
		}
	}

	fmt.Println("Client stopped.")
}
