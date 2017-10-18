package main

import (
	"encoding/json"
	"encoding/xml"
	"log"
	"testing"
	"time"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

const (
	rabbitAddress = "amqp://guest:guest@localhost:5672/"
	// rabbitAddress = "amqp://guest:guest@datdb.cphbusiness.dk:5672"
)

func TestJsonInput(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := amqp.Dial(rabbitAddress)
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	go startNormalizer(conn, make(chan bool))

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

	bankutil.Publish(ch, body, "", "cb_xml_bank_out")
	bankutil.Publish(ch, body2, "", "cb_xml_bank_out")
	bankutil.Publish(ch, body3, "", "cb_xml_bank_out")
	bankutil.Publish(ch, body4, "", "cb_xml_bank_out")
	bankutil.Publish(ch, body5, "", "cb_xml_bank_out")
	time.Sleep(500 * time.Millisecond)

	msgs, err := ch.Consume("aggregator", "", true, false, false, false, nil)
	m := <-msgs
	log.Println(string(m.Body))
	//TODO
}

func TestXMLInput(t *testing.T) {
	conn, err := amqp.Dial(rabbitAddress)
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
	body, _ := xml.Marshal(lr)
	body2, _ := xml.Marshal(lr2)
	body3, _ := xml.Marshal(lr3)
	body4, _ := xml.Marshal(lr4)
	body5, _ := xml.Marshal(lr5)

	bankutil.Publish(ch, body, "RouteExchange", "cb_xml_bank_out")
	time.Sleep(500 * time.Millisecond)
	bankutil.Publish(ch, body2, "", "cb_xml_bank_out")
	time.Sleep(500 * time.Millisecond)
	bankutil.Publish(ch, body3, "", "cb_xml_bank_out")
	bankutil.Publish(ch, body4, "", "cb_xml_bank_out")
	bankutil.Publish(ch, body5, "", "cb_xml_bank_out")
}

func TestBadInput(t *testing.T) {
	conn, err := amqp.Dial(rabbitAddress)
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	body := []byte("kuk")

	bankutil.Publish(ch, body, "", "cb_xml_bank_out")
}

func TestMultiQueue(t *testing.T) {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := amqp.Dial(rabbitAddress)
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	go startNormalizer(conn, make(chan bool))

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	lr := bankutil.LoanResponse{
		InterestRate: 4.5,
		Ssn:          123412345,
	}
	lr2 := bankutil.LoanResponse{
		InterestRate: 4.6,
		Ssn:          123412345,
	}
	body, _ := json.Marshal(lr)
	body2, _ := json.Marshal(lr2)

	bankutil.Publish(ch, body, "", "cb_xml_bank_out")
	time.Sleep(500 * time.Millisecond)
	bankutil.Publish(ch, body2, "", "ckkm-test-queue")
	msgs, err := ch.Consume("aggregator", "", true, false, false, false, nil)
	m := <-msgs
	m2 := <-msgs
	assert.Equal(t, `{"interestRate":4.5,"ssn":123412345,"bank":"Borum Bank"}`, string(m.Body))
	assert.Equal(t, `{"interestRate":4.6,"ssn":123412345,"bank":"Krissen Bank"}`, string(m2.Body))
}
