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
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
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

	bankutil.Publish(ch, body, "RouteExchange", "cb_xml_bank_in")

	msgs, err := ch.Consume("cb_xml_bank_out", "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Consume fail")

	select {
	case m := <-msgs:
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
