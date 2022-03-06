package main

import (
	"fmt"
	rmq "github.com/memnix/rabbitmq-tools"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	connection := new(rmq.RabbitMQConnection)

	err := connection.InitConnection("amqp://guest:guest@localhost:5672/", "logs")
	if err != nil {
		return
	}

	fmt.Println("Connected")

	defer func() {
		err := connection.CloseConnection()
		if err != nil {
			failOnError(err, "Failed to close connection")
		}
	}()

	defer func() {
		err := connection.CloseChannel()
		if err != nil {
			failOnError(err, "Failed to close channel")
		}
	}()

	queues := []rmq.Queue{{
		Keys: []string{"error.#"},
		Name: "error",
	}, {
		Keys: []string{"info.#"},
		Name: "info",
	}}

	err = connection.SetQueues(queues)
	if err != nil {
		return
	}

	forever := make(chan bool)

	deliveries, err := connection.Consume()
	if err != nil {
		panic(err)
	}

	for q, d := range deliveries {
		go func(q string, delivery <-chan amqp.Delivery) {
			for d := range delivery {
				log.Printf("Received a message: %s from %s", d.Body, q)
			}
		}(q, d)
	}

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
