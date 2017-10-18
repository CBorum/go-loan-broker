package main

import (
	"encoding/json"
	"encoding/xml"
	"log"
	"time"

	. "github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
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
		Responses: make(map[int][]*LoanResponse),
	}
	rl := make(map[int]int)

	_, err = StdQueueDeclareWithBind(ch, "route_meta", "LB4.RouteExchange")
	_, err = StdQueueDeclareWithBind(ch, "result_queue", "LB4.RouteExchange")

	q, err := StdQueueDeclare(ch, "aggregator")
	FailOnError(err, "Failed to declare queue")
	go startQueueConsumer(ch, q.Name, br, rl)
	go startRouteListener(ch, br, rl)
	log.Println("Requesting...")

	<-quit
}

func startRouteListener(ch *amqp.Channel, br *BankResponses, rl map[int]int) {
	msgs, err := ch.Consume("route_meta", "", true, false, false, false, nil)
	FailOnError(err, "Comsume fail")
	log.Println("Consume", "route_meta")
	ra := resultAmount{}
	for m := range msgs {
		err := json.Unmarshal(m.Body, ra)
		if err != nil {
			log.Println(err)
		} else {
			rl[ra.Ssn] = ra.Amount
			time.AfterFunc(2500*time.Millisecond, publishResponseFunc(ch, br, rl, ra.Ssn))
		}
	}
}

func startQueueConsumer(ch *amqp.Channel, queueName string, br *BankResponses, rl map[int]int) {
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
				time.AfterFunc(2000*time.Millisecond, publishResponseFunc(ch, br, rl, le.Ssn))
			}

			// If amount of result match the requests sendt
			if val, ok := rl[le.Ssn]; ok {
				if val == len(br.Responses[le.Ssn]) {
					log.Println("request satisfied")
					publichResponse(ch, br, rl, le.Ssn)
				}
			}

			log.Println(br.Responses)
			br.Unlock()
		}(m.Body)
	}
}

type responseFunc func()

func publishResponseFunc(ch *amqp.Channel, br *BankResponses, rl map[int]int, ssn int) responseFunc {
	return func() {
		publichResponse(ch, br, rl, ssn)
	}
}

func publichResponse(ch *amqp.Channel, br *BankResponses, rl map[int]int, ssn int) {
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
			Publish(ch, body, "LB4.RouteExchange", "result_queue")
		}
	}
	delete(rl, ssn)
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
	Ssn    int `json:"ssn"`
	Amount int `json:"amount"`
}
