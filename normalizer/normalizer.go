package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

var (
	responseQueues = [...]string{"cb_xml_bank_out"}
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	br := &bankutil.BankResponses{
		Responses: make(map[int][]*bankutil.LoanResponse),
	}

	for _, rq := range responseQueues {
		_, err = bankutil.StdQueueDeclare(ch, rq)
		bankutil.FailOnError(err, "Failed to declare queue")
		go startQueueConsumer(ch, rq, br)
	}

	log.Println("Requesting...")

	quit := make(chan bool)
	<-quit
}

func startQueueConsumer(ch *amqp.Channel, queueName string, br *bankutil.BankResponses) {
	msgs, err := ch.Consume(queueName, "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Consume fail")

	log.Println("Consume", queueName)
	for m := range msgs {
		go func(body []byte) {
			log.Println(string(body))
			le := &bankutil.LoanResponse{}
			err := json.Unmarshal(body, le)
			if err != nil {
				log.Println(err)
				return
			}

			br.Lock()
			if responses, ok := br.Responses[le.Ssn]; ok {
				x := false
				for _, val := range responses {
					if val.Bank == le.Bank {
						x = true
					}
				}
				if !x {
					br.Responses[le.Ssn] = append(responses, le)
				}
			} else {
				br.Responses[le.Ssn] = append(responses, le)
				time.AfterFunc(2*time.Second, publishResponse(ch, br, le.Ssn))
			}

			log.Println(br.Responses)
			br.Unlock()
		}(m.Body)
	}
}

type responseFunc func()

func publishResponse(ch *amqp.Channel, br *bankutil.BankResponses, ssn int) responseFunc {
	return func() {
		br.Lock()
		defer br.Unlock()
		body, err := json.Marshal(br.Responses[ssn])
		if err != nil {
			log.Println(err)
		} else {
			log.Println(string(body))
			// bankutil.Publish(ch, body, "RoutingExchange", "aggreation")
		}
		delete(br.Responses, ssn)
	}
}
