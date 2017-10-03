package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func rpc() (res *LoanResponse, err error) {
	conn, err := amqp.Dial("amqp://guest:guest@datdb.cphbusiness.dk:5672")
	// conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	exchangeName := "cphbusiness.bankJSON"
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		false,        // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,       // queue bind name
		"",           // queue routing key
		exchangeName, // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind queue")

	q2, err := ch.QueueDeclare(
		"rpc_out", // name
		false,     // durable
		true,      // delete when unused
		true,      // exclusive
		false,     // noWait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q2.Name,      // queue bind name
		"",           // queue routing key
		exchangeName, // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind queue")

	msgs, err := ch.Consume(
		"rpc_out", // queue
		"",        // consumer
		true,      // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to register a consumer")

	corrId := randomString(32)
	lr := &LoanRequest{123412342, 650, 4234.54, 1}
	body, err := json.Marshal(lr)

	err = ch.Publish(
		exchangeName, // exchange
		"",           // routing key //rpc_queue
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:   "text/json",
			CorrelationId: corrId,
			ReplyTo:       "rpc_out",
			Body:          body,
		})
	failOnError(err, "Failed to publish a message")

	for d := range msgs {
		res = &LoanResponse{}
		err = json.Unmarshal(d.Body, res)
		if res.InterestRate != 0 {
			log.Println(res)
			failOnError(err, "Failed to convert body to integer")
			break
		}
	}
	return
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	rand.Seed(time.Now().UTC().UnixNano())

	log.Println(" [x] Requesting")
	res, err := rpc()
	failOnError(err, "Failed to handle RPC request")

	log.Printf(" [.] Got %#v", res)
}

type LoanRequest struct {
	Ssn          int     `xml:"ssn" json:"ssn"`
	CreditScore  int     `xml:"creditScore" json:"creditScore"`
	LoanAmount   float64 `xml:"loanAmount" json:"loanAmount"`
	LoanDuration int     `xml:"loanDuration" json:"loanDuration"`
}

type LoanResponse struct {
	InterestRate float64 `xml:"interestRate" json:"interestRate"`
	Ssn          int     `xml:"ssn" json:"ssn"`
}
