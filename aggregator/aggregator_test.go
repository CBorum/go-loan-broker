package main

import (
	"encoding/json"
	"log"
	"reflect"
	"testing"
	"time"

	. "github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

func TestJsonInput(t *testing.T) {
	quit := make(chan bool)
	conn, err := amqp.Dial(RabbitURL)
	FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	go startAggregator(conn, quit)

	ch, err := conn.Channel()
	FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := StdQueueDeclare(ch, AggregatorName)
	FailOnError(err, "Failed to declare queue")

	lr := &LoanResponse{
		InterestRate: 4.5,
		Ssn:          123412345,
	}
	lr2 := &LoanResponse{3.2, 123412345, "bank1"}
	lr3 := &LoanResponse{7.5, 123412345, "bank2"}
	lr4 := &LoanResponse{5.4, 123412345, "bank3"}
	lr5 := &LoanResponse{4.3, 123412345, "bank4"}
	body, _ := json.Marshal(lr)
	body2, _ := json.Marshal(lr2)
	body3, _ := json.Marshal(lr3)
	body4, _ := json.Marshal(lr4)
	body5, _ := json.Marshal(lr5)

	Publish(ch, body, "", q.Name)
	time.Sleep(500 * time.Millisecond)
	Publish(ch, body2, "", q.Name)
	time.Sleep(500 * time.Millisecond)
	Publish(ch, body3, "", q.Name)
	Publish(ch, body4, "", q.Name)
	Publish(ch, body5, "", q.Name)

	msgs, err := ch.Consume("ckkm-result-queue", "", true, false, false, false, nil)
	if err != nil {
		t.FailNow()
	}
	select {
	case m := <-msgs:
		le := &LoanResponse{}
		err := json.Unmarshal(m.Body, le)
		if err != nil {
			log.Println(err)
			t.FailNow()
		}
		log.Println(le, lr3)
		if !reflect.DeepEqual(le, lr3) {
			t.FailNow()
		}
	case <-time.After(time.Second * 3):
		t.FailNow()
	}
}
