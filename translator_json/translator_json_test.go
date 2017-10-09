package main

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

func TestJsonInput(t *testing.T) {
	// conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	conn, err := amqp.Dial("amqp://guest:guest@datdb.cphbusiness.dk")
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	lr := bankutil.LoanRequest{
		Ssn:          123412345,
		CreditScore:  650,
		LoanAmount:   4554.5,
		LoanDuration: 123,
	}
	body, err := json.Marshal(lr)
	if err != nil {
		t.FailNow()
	}

	exchangeName := "LB4.RouteExchange"
	err = bankutil.StdExchangeDeclare(ch, exchangeName)
	bankutil.FailOnError(err, "Failed to declare exchange")

	_, err = bankutil.StdQueueDeclareWithBind(ch, "json_bank_in", exchangeName)
	bankutil.FailOnError(err, "Failed to declare queue")

	_, err = bankutil.StdQueueDeclareWithBind(ch, "json_bank_out", exchangeName)
	bankutil.FailOnError(err, "Failed to declare queue")

	bankutil.Publish(ch, body, exchangeName, "json_bank_in")

	msgs, err := ch.Consume("json_bank_out", "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Consume fail")

	select {
	case m := <-msgs:
		log.Println(string(m.Body))
		le := &bankutil.LoanResponse{}
		err := json.Unmarshal(m.Body, le)
		if err != nil {
			log.Println(err)
			t.FailNow()
		}
		if le.InterestRate == 0 || le.Ssn != lr.Ssn {
			log.Println(le.InterestRate, le.Ssn)
			t.FailNow()
		}
		log.Println(le)
	case <-time.After(time.Duration(1 * time.Second)):
		log.Println("timeout")
		t.FailNow()
	}
}
