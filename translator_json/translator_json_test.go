package main

import (
	"encoding/json"
	"log"
	"testing"
	"time"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

func TestJsonInput(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := amqp.Dial(bankutil.RabbitURL)
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

	bankutil.Publish(ch, body, "", "ckkm-cph-json")

	msgs, err := ch.Consume("ckkm-cph-json-out", "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Consume fail")

	select {
	case m := <-msgs:
		log.Println(string(m.Body))
		le := &bankutil.LoanResponse{}
		err := json.Unmarshal(m.Body, le)
		assert.Nil(t, err)
		assert.NotEqual(t, 0, le.InterestRate)
		assert.Equal(t, le.Ssn, lr.Ssn)
		log.Println(le)
	case <-time.After(1 * time.Second):
		log.Println("timeout")
		t.FailNow()
	}
}
func TestSsnString(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := amqp.Dial(bankutil.RabbitURL)
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	s := []byte(`{"ssn":"160578-9787","creditScore":598,"loanAmount":10.0,"loanDuration":360}`)
	ch.Publish(
		"cphbusiness.bankJSON", // exchange
		"",    // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "text/json",
			ReplyTo:     "ckkm-cph-json-out",
			Body:        s,
		})

	msgs, err := ch.Consume("ckkm-cph-json-out", "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Consume fail")

	select {
	case m := <-msgs:
		log.Println(string(m.Body))
	case <-time.After(1 * time.Second):
		log.Println("timeout")
		t.FailNow()
	}
}
