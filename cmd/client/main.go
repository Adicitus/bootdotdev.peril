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
	fmt.Println("Starting Peril client...")

	mqConnection := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(mqConnection)

	if err != nil {
		fmt.Printf("Failed to connect wiht  MQ server: %s\n", err.Error())
		os.Exit(1)
	}

	defer conn.Close()

	pubCh, err := conn.Channel()

	if err != nil {
		fmt.Printf("Failed to establish a channel for message publishing: %s\n", err.Error())
		os.Exit(1)
	}

	username := ""

	if len(os.Args) > 1 {
		username = os.Args[1]
		gamelogic.ClientWelcome(username)
	} else {
		username, err = gamelogic.ClientWelcome("")

		if err != nil {
			fmt.Printf("Failed to display greeting: %s\n", err.Error())
			os.Exit(1)
		}
	}

	state := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, fmt.Sprintf("pause.%s", username), routing.PauseKey, false, true, handlerPause(state))

	if err != nil {
		fmt.Printf("Failed to subscibe for pause messages: %s\n", err.Error())
		os.Exit(1)
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", username), "army_moves.*", false, true, handlerArmyMove(state, pubCh))

	if err != nil {
		fmt.Printf("Failed to subscibe for army moves messages: %s\n", err.Error())
		os.Exit(1)
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix), true, false, handlerWarRecognition(state))

	if err != nil {
		fmt.Printf("Failed to subscibe for war messages: %s\n", err.Error())
		os.Exit(1)
	}

GameLoop:
	for {
		input := gamelogic.GetInput(username)

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

			pubsub.PublishJSON(pubCh, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", username), move)

			fmt.Printf("Published move %d to %s\n", len(move.Units), move.ToLocation)

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

func handlerPause(state *gamelogic.GameState) func(routing.PlayingState) (bool, bool) {
	return func(ps routing.PlayingState) (ack, requeue bool) {
		fmt.Println("Handling pause...")
		defer fmt.Printf("%s > ", state.GetUsername())
		state.HandlePause(ps)
		return true, false
	}
}

func handlerArmyMove(state *gamelogic.GameState, pubCh *amqp.Channel) func(gamelogic.ArmyMove) (bool, bool) {
	return func(move gamelogic.ArmyMove) (ack, requeue bool) {
		fmt.Println("Handling move...")
		outcome := state.HandleMove(move)
		fmt.Printf("%s > ", state.GetUsername())

		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return true, false
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(pubCh, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, state.Player.Username), gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: state.GetPlayerSnap(),
			})

			if err != nil {
				return false, true
			}
			return true, false
		default:
			return false, false
		}
	}
}

func handlerWarRecognition(state *gamelogic.GameState) func(gamelogic.RecognitionOfWar) (bool, bool) {
	return func(row gamelogic.RecognitionOfWar) (bool, bool) {
		defer fmt.Printf("%s > ", state.GetUsername())

		outcome, _, _ := state.HandleWar(row)

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return false, true
		case gamelogic.WarOutcomeNoUnits:
			return false, false
		case gamelogic.WarOutcomeOpponentWon:
			return true, false
		case gamelogic.WarOutcomeYouWon:
			return true, false
		case gamelogic.WarOutcomeDraw:
			return true, false
		default:
			fmt.Printf("Unexpected war outcome: %v\n", outcome)
			return false, false
		}
	}
}
