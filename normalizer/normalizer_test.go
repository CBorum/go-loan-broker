package main

import (
	"encoding/json"
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

	lr := bankutil.LoanResponse{
		InterestRate: 4.5,
		Ssn:          123412345,
	}
	lr2 := bankutil.LoanResponse{3.2, 223412345, "bank1"}
	lr3 := bankutil.LoanResponse{5.5, 123412345, "bank2"}
	lr4 := bankutil.LoanResponse{5.5, 123412345, "bank3"}
	lr5 := bankutil.LoanResponse{4.5, 123412345, "bank4"}
	body, _ := json.Marshal(lr)
	body2, _ := json.Marshal(lr2)
	body3, _ := json.Marshal(lr3)
	body4, _ := json.Marshal(lr4)
	body5, _ := json.Marshal(lr5)

	bankutil.Publish(ch, body, "RouteExchange", "cb_xml_bank_out")
	time.Sleep(500 * time.Millisecond)
	bankutil.Publish(ch, body2, "RouteExchange", "cb_xml_bank_out")
	time.Sleep(500 * time.Millisecond)
	bankutil.Publish(ch, body3, "RouteExchange", "cb_xml_bank_out")
	bankutil.Publish(ch, body4, "RouteExchange", "cb_xml_bank_out")
	bankutil.Publish(ch, body5, "RouteExchange", "cb_xml_bank_out")
}
