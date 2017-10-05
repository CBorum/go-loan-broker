package main

import (
	"encoding/xml"
	"log"
	"math/rand"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	exchangeName := "BigMoneyBankingXML"
	err = bankutil.StdExchangeDeclare(ch, exchangeName)
	bankutil.FailOnError(err, "Failed to declare an exchange")

	q, err := bankutil.StdQueueDeclareWithBind(ch, "rpc_queue", exchangeName)
	bankutil.FailOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Failed to register a consumer")

	log.Printf(" [*] Awaiting RPC requests")

	for d := range msgs {
		lr := &bankutil.LoanRequest{}
		err := xml.Unmarshal(d.Body, lr)
		bankutil.FailOnError(err, "Failed to unmarshal loan request")

		log.Printf(" [.] %#v", lr)
		le := &bankutil.LoanResponse{
			InterestRate: rand.Float64()*5 + 3,
			Ssn:          lr.Ssn,
		}
		body, err := xml.Marshal(le)
		bankutil.FailOnError(err, "Failed to marshal loan response")
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
		bankutil.FailOnError(err, "Failed to publish a message")

		// d.Ack(false)
	}
}
