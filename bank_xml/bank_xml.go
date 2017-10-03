package main

import (
	"encoding/xml"
	"log"
	"math/rand"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	exchangeName := "BigMoneyBankingXML"
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		"rpc_queue", // name
		false,       // durable
		false,       // delete when unused
		false,       // exclusive
		false,       // no-wait
		nil,         // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,       // queue bind name
		"rpc_queue",  // queue routing key
		exchangeName, // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind queue")

	// err = ch.Qos(
	// 	1,     // prefetch count
	// 	0,     // prefetch size
	// 	false, // global
	// )
	// failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			lr := &LoanRequest{}
			err := xml.Unmarshal(d.Body, lr)
			failOnError(err, "Failed to unmarshal loan request")

			log.Printf(" [.] %#v", lr)
			le := &LoanResponse{getInterestRate(lr), lr.Ssn}
			body, err := xml.Marshal(le)
			failOnError(err, "Failed to marshal loan response")
			log.Println("returning", string(body))
			err = ch.Publish(
				exchangeName, // exchange
				d.ReplyTo,    // routing key
				false,        // mandatory
				false,        // immediate
				amqp.Publishing{
					ContentType:   "text/xml",
					CorrelationId: d.CorrelationId,
					Body:          body,
				})
			failOnError(err, "Failed to publish a message")

			// d.Ack(false)
		}
	}()

	log.Printf(" [*] Awaiting RPC requests")
	<-forever
}

func getInterestRate(lr *LoanRequest) float64 {
	return rand.Float64()*5 + 3
}

type LoanRequest struct {
	Ssn          string  `xml:"ssn" json:"ssn"`
	CreditScore  int     `xml:"creditScore" json:"creditScore"`
	LoanAmount   float64 `xml:"loanAmount" json:"loanAmount"`
	LoanDuration string  `xml:"loanDuration" json:"loanDuration"`
}

type LoanResponse struct {
	InterestRate float64 `xml:"interestRate" json:"interestRate"`
	Ssn          string  `xml:"ssn" json:"ssn"`
}
