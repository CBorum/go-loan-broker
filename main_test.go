package test

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
		Ssn:          "12345-1234",
		CreditScore:  650,
		LoanAmount:   4554.5,
		LoanDuration: 123,
	}

	body, err := json.Marshal(lr)
	if err != nil {
		t.FailNow()
	}
	bankutil.Publish(ch, body, "", "ckkm-cph-json")
	// bankutil.Publish(ch, body, "", "ckkm-xml-in")

	q, err := bankutil.StdQueueDeclare(ch, "ckkm-result-queue")
	bankutil.FailOnError(err, "Faield to declare queue")

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Consume fail")

	select {
	case m := <-msgs:
		le := &bankutil.LoanResponse{}
		err := json.Unmarshal(m.Body, le)
		assert.Nil(t, err)
		assert.NotEqual(t, 0, le.InterestRate)
		assert.Equal(t, lr.Ssn, le.Ssn)
		log.Println(le)
	case <-time.After(time.Duration(3 * time.Second)):
		t.FailNow()
	}
}

func TestJsonInputWithRouteMeta(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := amqp.Dial(bankutil.RabbitURL)
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	lr := bankutil.LoanRequest{
		Ssn:          "12345-1234",
		CreditScore:  650,
		LoanAmount:   4554.5,
		LoanDuration: 123,
	}

	body, err := json.Marshal(lr)
	if err != nil {
		t.FailNow()
	}
	bankutil.Publish(ch, body, "", "ckkm-cph-json")
	bankutil.Publish(ch, body, "", "ckkm-xml-in")

	ra := &resultAmount{"123412345", 2}
	body, err = json.Marshal(ra)
	if err != nil {
		t.FailNow()
	}
	log.Println(string(body))
	bankutil.Publish(ch, body, "", "ckkm-route-meta")

	q, err := bankutil.StdQueueDeclare(ch, "ckkm-result-queue")
	bankutil.FailOnError(err, "Faield to declare queue")

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Consume fail")

	select {
	case m := <-msgs:
		le := &bankutil.LoanResponse{}
		err := json.Unmarshal(m.Body, le)
		assert.Nil(t, err)
		assert.NotEqual(t, 0, le.InterestRate)
		assert.Equal(t, lr.Ssn, le.Ssn)
		log.Println(le)
	case <-time.After(time.Duration(3 * time.Second)):
		t.FailNow()
	}
}

type resultAmount struct {
	Ssn    string `json:"ssn"`
	Amount int    `json:"amount"`
}
