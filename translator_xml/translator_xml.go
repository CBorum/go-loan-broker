package main

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/streadway/amqp"
)

const (
	bankExchange  = "BigMoneyBankingXML"
	routeExchange = "RouteExchange"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	rand.Seed(time.Now().UTC().UnixNano())
	log.Println(" [x] Requesting")

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	rpcQueue := getQueue(ch, bankExchange, "rpc_in")
	routeQueue := getQueue(ch, routeExchange, "cb_xml_bank_in")
	_ = getQueue(ch, routeExchange, "cb_xml_bank_out")

	inMsgs, err := ch.Consume(routeQueue.Name, "", true, false, false, false, nil)
	failOnError(err, "Failed to register a consumer")

	for m := range inMsgs {
		log.Println(string(m.Body))
		le, err := handleInMsg(m.Body, rpcQueue, ch)
		if err != nil {
			log.Println(err)
		}
		log.Println(le)
		err = publishLoanResponse(le, ch)
		if err != nil {
			log.Println(err)
		}
	}
}

func handleInMsg(body []byte, rpcQueue amqp.Queue, ch *amqp.Channel) (le *LoanResponse, err error) {
	lr := &LoanRequest{}
	err = json.Unmarshal(body, lr)
	if err != nil {
		return nil, err
	}

	corrId := randomString(32)
	xmlBody, err := xml.Marshal(lr)

	err = ch.Publish(
		bankExchange, // exchange
		"rpc_queue",  // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:   "text/xml",
			CorrelationId: corrId,
			ReplyTo:       rpcQueue.Name, //TODO change this to the queue normalizer will listen to
			Body:          xmlBody,
		})
	if err != nil {
		return nil, err
	}

	bankMsgs, err := ch.Consume(rpcQueue.Name, "abc", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	for d := range bankMsgs {
		log.Println(string(d.Body))
		if corrId == d.CorrelationId {
			le = &LoanResponse{}
			err = xml.Unmarshal(d.Body, le)
			if err != nil {
				return nil, err
			}
			break
		}
	}
	ch.Cancel("abc", false)

	return le, err
}

func publishLoanResponse(le *LoanResponse, ch *amqp.Channel) error {
	if le == nil {
		return errors.New("asdf")
	}

	jsonBody, err := json.Marshal(le)
	if err != nil {
		return err
	}

	err = ch.Publish(
		routeExchange,     // exchange
		"cb_xml_bank_out", // routing key
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType: "text/json",
			Body:        jsonBody,
		})
	return err
}

func getQueue(ch *amqp.Channel, exchangeName string, queueName string) amqp.Queue {
	err := ch.ExchangeDeclare(
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
		queueName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,       // queue bind name
		queueName,    // queue routing key
		exchangeName, // exchange
		false,        // no wait
		nil,          // table
	)
	failOnError(err, "Failed to bind queue")
	return q
}

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

/*
{
	"ssn": "1234123412",
	"creditScore": 650,
	"loanAmount": 4234.54,
	"loanDuration": "1973-09-15 01:00:00.0 CET"
}
*/
