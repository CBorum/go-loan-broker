package main

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"log"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

var (
	responseQueues = [...]string{"cb_xml_bank_out"}
	// responseQueues = [...]string{"cb_xml_bank_out", "ckkm-test-queue"}
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	quit := make(chan bool)
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	// conn, err := amqp.Dial("amqp://guest:guest@datdb.cphbusiness.dk:5672")
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	for _, rq := range responseQueues {
		_, err = bankutil.StdQueueDeclare(ch, rq)
		bankutil.FailOnError(err, "Failed to declare queue")
		go startQueueConsumer(ch, rq)
	}

	log.Println("Requesting...")
	<-quit
}

func startQueueConsumer(ch *amqp.Channel, queueName string) {
	log.Println("Consume", queueName)
	msgs, err := ch.Consume(queueName, "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Consume fail")

	for m := range msgs {
		go func(body []byte) {
			log.Println(string(body))
			le, err := parseLoanResponse(body)
			if err != nil {
				log.Println(err)
				return
			}
			//set bank name

			jsonBody, err := json.Marshal(le)
			if err != nil {
				log.Println(err)
				return
			}
			log.Println("sendt", string(jsonBody))
			err = bankutil.Publish(ch, jsonBody, "", "aggregator")
			if err != nil {
				log.Println(err)
			}
		}(m.Body)
	}
}

func parseLoanResponse(body []byte) (le *bankutil.LoanResponse, err error) {
	le = &bankutil.LoanResponse{}
	err = json.Unmarshal(body, le)
	if err != nil {
		err = xml.Unmarshal(body, le)
		if err != nil {
			return
		}
	}
	if le.InterestRate == 0 || le.Ssn == 0 {
		err = errors.New("Malformed request")
	}
	return
}
