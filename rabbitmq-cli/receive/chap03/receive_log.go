package main

import (
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err.Error())
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to declare a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // kind
		true,     // durable
		false,    // auto-delete
		false,    //internal
		false,    // no-wait
		nil,      // args
	)
	failOnError(err, "Failed to declare a exchange")

	q, err := ch.QueueDeclare(
		"",    // queue name
		false, // durable
		false, // auto-delete
		true,  // exclusive
		false, // no-wait
		nil,   // args
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name, // queue name
		"",     // routing key
		"logs", // exchange name
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to bind queue to exchange")

	msgs, err := ch.Consume(
		q.Name, // queue name
		"",     // consumer
		true,   // auto-ack
		false,
		false,
		false,
		nil,
	)
	failOnError(err, "Failed to register a consumer")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
		}
	}()

	log.Printf(" [*] is running...")
	wg.Wait()
}
