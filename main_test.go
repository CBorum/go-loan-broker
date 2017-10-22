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
		Ssn:          123412345,
		CreditScore:  650,
		LoanAmount:   4554.5,
		LoanDuration: 123,
	}
	body, err := json.Marshal(lr)
	if err != nil {
		t.FailNow()
	}

	bankutil.Publish(ch, body, "", "ckkm-xml-in")

	msgs, err := ch.Consume("ckkm-result-queue", "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Consume fail")

	select {
	case m := <-msgs:
		le := &bankutil.LoanResponse{}
		err := json.Unmarshal(m.Body, le)
		assert.Nil(t, err)
		assert.NotEqual(t, 0, le.InterestRate)
		assert.Equal(t, le.Ssn, lr.Ssn)
		log.Println(le)
	case <-time.After(time.Duration(3 * time.Second)):
		t.FailNow()
	}
}
