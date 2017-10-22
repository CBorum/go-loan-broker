package main

import (
	"encoding/json"
	"encoding/xml"
	"log"
	"math/rand"
	"time"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

const (
	bankExchange = "BigMoneyBankingXML"
	bankIn       = "ckkm-xml-in"
	bankOut      = "ckkm-xml-out"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	rand.Seed(time.Now().UTC().UnixNano())
	log.Println(" [x] Requesting")

	conn, err := amqp.Dial(bankutil.RabbitURL)
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = bankutil.StdExchangeDeclare(ch, bankExchange)
	bankutil.FailOnError(err, "Failed to declare exchange")

	routeQueue, err := bankutil.StdQueueDeclare(ch, bankIn)
	bankutil.FailOnError(err, "Failed to declare queue")

	replyQueue, err := bankutil.StdQueueDeclare(ch, bankOut)
	bankutil.FailOnError(err, "Failed to declare queue")

	inMsgs, err := ch.Consume(routeQueue.Name, "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Failed to register a consumer")

	for m := range inMsgs {
		log.Println(string(m.Body))
		err = handleInMsg(m.Body, replyQueue, ch)
		if err != nil {
			log.Println(err)
		}
	}
}

func handleInMsg(body []byte, replyQueue amqp.Queue, ch *amqp.Channel) error {
	lr := &bankutil.LoanRequest{}
	err := json.Unmarshal(body, lr)
	if err != nil {
		return err
	}

	corrID := randomString(32)
	xmlBody, err := xml.Marshal(lr)

	return ch.Publish(
		bankExchange, // exchange
		"rpc_queue",  // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:   "text/xml",
			CorrelationId: corrID,
			ReplyTo:       replyQueue.Name, //TODO change this to the queue normalizer will listen to
			Body:          xmlBody,
		})
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

/*
{
	"ssn": "1234123412",
	"creditScore": 650,
	"loanAmount": 4234.54,
	"loanDuration": "1973-09-15 01:00:00.0 CET"
}
*/
