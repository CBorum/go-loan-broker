package main

import (
	"encoding/json"
	"encoding/xml"
	"log"
	"time"

	. "github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

var (
	timers map[string]*time.Timer
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := amqp.Dial(RabbitURL)
	FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	quit := make(chan bool)
	startAggregator(conn, quit)
}

func startAggregator(conn *amqp.Connection, quit chan bool) {
	ch, err := conn.Channel()
	FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	br := &BankResponses{
		Responses: make(map[string][]*LoanResponse),
	}
	rl := make(map[string]int)
	timers = make(map[string]*time.Timer)

	_, err = StdQueueDeclare(ch, "ckkm-route-meta")
	FailOnError(err, "Failed to declare queue")
	_, err = StdQueueDeclare(ch, "ckkm-result-queue")
	FailOnError(err, "Failed to declare queue")
	q, err := StdQueueDeclare(ch, AggregatorName)
	FailOnError(err, "Failed to declare queue")

	go startQueueConsumer(ch, q.Name, br, rl)
	go startRouteListener(ch, br, rl)
	log.Println("Requesting...")

	<-quit
}

func startRouteListener(ch *amqp.Channel, br *BankResponses, rl map[string]int) {
	msgs, err := ch.Consume("ckkm-route-meta", "", true, false, false, false, nil)
	FailOnError(err, "Comsume fail")
	log.Println("Consume", "ckkm-route-meta")
	ra := &resultAmount{}
	for m := range msgs {
		log.Println(string(m.Body))
		err := json.Unmarshal(m.Body, ra)
		if err != nil {
			log.Println(err)
		} else {
			rl[ra.Ssn] = ra.Amount
			//TODO
			// time.AfterFunc(2200*time.Millisecond, publishResponseFunc(ch, br, rl, ra.Ssn))
		}
	}
}

func startQueueConsumer(ch *amqp.Channel, queueName string, br *BankResponses, rl map[string]int) {
	msgs, err := ch.Consume(queueName, "", true, false, false, false, nil)
	FailOnError(err, "Consume fail")

	log.Println("Consume", queueName)
	for m := range msgs {
		go func(body []byte) {
			log.Println(string(body))
			le, err := parseLoanResponse(body)
			if err != nil {
				log.Println(err)
				return
			}

			br.Lock()
			if responses, ok := br.Responses[le.Ssn]; ok {
				br.Responses[le.Ssn] = append(responses, le)
			} else {
				br.Responses[le.Ssn] = append(responses, le)
				timers[le.Ssn] = time.AfterFunc(2000*time.Millisecond, publishResponseFunc(ch, br, rl, le.Ssn))
				log.Println(timers)
			}
			br.Unlock()
			log.Println(br.Responses)

			// If amount of result match the requests sendt
			if val, ok := rl[le.Ssn]; ok {
				if val == len(br.Responses[le.Ssn]) {
					log.Println("request satisfied")
					publichResponse(ch, br, rl, le.Ssn)
				}
			}
		}(m.Body)
	}
}

type responseFunc func()

func publishResponseFunc(ch *amqp.Channel, br *BankResponses, rl map[string]int, ssn string) responseFunc {
	return func() {
		publichResponse(ch, br, rl, ssn)
	}
}

func publichResponse(ch *amqp.Channel, br *BankResponses, rl map[string]int, ssn string) {
	br.Lock()
	defer br.Unlock()
	if _, ok := br.Responses[ssn]; ok {

		max := &LoanResponse{}
		for _, v := range br.Responses[ssn] {
			if v.InterestRate > max.InterestRate {
				max = v
			}
		}

		body, err := json.Marshal(max)
		if err != nil {
			log.Println(err)
		} else {
			log.Println("sendt", string(body))
			Publish(ch, body, "", "ckkm-result-queue")
			if _, ok := timers[ssn]; ok {
				timers[ssn].Stop()
				delete(timers, ssn)
				log.Println("timer stopped")
			}
		}
	}
	log.Println("delete rl", ssn)
	delete(rl, ssn)
	log.Println("delete br", ssn)
	delete(br.Responses, ssn)
}

func parseLoanResponse(body []byte) (le *LoanResponse, err error) {
	le = &LoanResponse{}
	err = json.Unmarshal(body, le)
	if err != nil {
		err = xml.Unmarshal(body, le)
		return
	}
	return
}

type resultAmount struct {
	Ssn    string `json:"ssn"`
	Amount int    `json:"amount"`
}
