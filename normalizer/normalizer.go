package main

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"log"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	quit := make(chan bool)
	conn, err := amqp.Dial(bankutil.RabbitURL)
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	startNormalizer(conn, quit)
}

func startNormalizer(conn *amqp.Connection, quit chan bool) {
	responseQueues := make(map[string]string)
	responseQueues["ckkm-xml-out"] = "BorumBorum Bank"
	responseQueues["ckkm-PremiumBank-response"] = "Premium Bank"
	responseQueues["ckkm-cph-json-out"] = "cphbusiness.BankJSON"

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	for qName, bName := range responseQueues {
		_, err = bankutil.StdQueueDeclare(ch, qName)
		bankutil.FailOnError(err, "Failed to declare queue")
		go startQueueConsumer(ch, qName, bName)
	}

	log.Println("Requesting...")
	<-quit
}

func startQueueConsumer(ch *amqp.Channel, queueName string, bankname string) {
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
			le.Bank = bankname

			jsonBody, err := json.Marshal(le)
			if err != nil {
				log.Println(err)
				return
			}
			log.Println("sendt", string(jsonBody))
			err = bankutil.Publish(ch, jsonBody, "", bankutil.AggregatorName)
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
