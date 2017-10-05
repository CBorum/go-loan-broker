package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

func rpc() (res *bankutil.LoanResponse, err error) {
	conn, err := amqp.Dial("amqp://guest:guest@datdb.cphbusiness.dk:5672")
	// conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	exchangeName := "cphbusiness.bankJSON"
	// err = ch.ExchangeDeclare(exchangeName, "fanout", false, false, false, false, nil)
	// bankutil.FailOnError(err, "Failed to declare an exchange")

	q, err := bankutil.StdQueueDeclare(ch, "rpc_out")
	bankutil.FailOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		true,   // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	bankutil.FailOnError(err, "Failed to register a consumer")

	lr := &bankutil.LoanRequest{
		Ssn:          653412342,
		CreditScore:  650,
		LoanAmount:   4234.54,
		LoanDuration: 1,
	}
	body, err := json.Marshal(lr)

	err = bankutil.PublishWithReply(ch, body, exchangeName, "", q.Name)
	bankutil.FailOnError(err, "Failed to publish a message")

	for d := range msgs {
		res = &bankutil.LoanResponse{}
		err = json.Unmarshal(d.Body, res)
		if err == nil && res.InterestRate != 0 {
			bankutil.FailOnError(err, "Failed to convert body to integer")
			break
		} else if err != nil {
			log.Println(err)
		}
	}

	return
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	rand.Seed(time.Now().UTC().UnixNano())

	log.Println(" [x] Requesting")
	res, err := rpc()
	bankutil.FailOnError(err, "Failed to handle RPC request")

	log.Printf(" [.] Got %#v", res)
}
