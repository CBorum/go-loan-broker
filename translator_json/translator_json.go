package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

const (
	bankExchange = "cphbusiness.bankJSON"
	bankIn       = "ckkm-cph-json"
	bankOut      = "ckkm-cph-json-out"
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
	ssn, err := strconv.Atoi(strings.Replace(lr.Ssn, "-", "", -1))
	if err != nil {
		return err
	}
	cphlr := &bankutil.CPHLoanRequest{
		Ssn:          ssn,
		LoanAmount:   lr.LoanAmount,
		LoanDuration: lr.LoanDuration,
		CreditScore:  lr.CreditScore,
	}

	jsonBody, err := json.Marshal(cphlr)
	if err != nil {
		return err
	}

	return ch.Publish(
		bankExchange, // exchange
		"",           // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/json",
			ReplyTo:     replyQueue.Name,
			Body:        jsonBody,
		})
}
