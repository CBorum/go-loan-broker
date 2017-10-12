package main

import (
	"encoding/json"
	"encoding/xml"
	"log"
	"time"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
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
	rl := make(map[int]int)

	_, err = bankutil.StdQueueDeclareWithBind(ch, "route_meta", "LB4.RouteExchange")

	q, err := bankutil.StdQueueDeclare(ch, "aggregator")
	bankutil.FailOnError(err, "Failed to declare queue")
	go startQueueConsumer(ch, q.Name, br, rl)
	go startRouteListener(ch, rl)
	log.Println("Requesting...")

	quit := make(chan bool)
	<-quit
}

func startRouteListener(ch *amqp.Channel, rl map[int]int) {
	msgs, err := ch.Consume("route_meta", "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Comsume fail")
	log.Println("Consume", "route_meta")
	ra := resultAmount{}
	for m := range msgs {
		err := json.Unmarshal(m.Body, ra)
		if err != nil {
			log.Println(err)
		} else {
			rl[ra.Ssn] = ra.Amount
		}
	}
}

func startQueueConsumer(ch *amqp.Channel, queueName string, br *bankutil.BankResponses, rl map[int]int) {
	msgs, err := ch.Consume(queueName, "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Consume fail")

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
				time.AfterFunc(2*time.Second, publishResponseFunc(ch, br, rl, le.Ssn))
			}

			// If amount of result match the requests sendt
			if val, ok := rl[le.Ssn]; ok {
				if val == len(br.Responses[le.Ssn]) {
					publichResponse(ch, br, rl, le.Ssn)
				}
			}

			log.Println(br.Responses)
			br.Unlock()
		}(m.Body)
	}
}

type responseFunc func()

func publishResponseFunc(ch *amqp.Channel, br *bankutil.BankResponses, rl map[int]int, ssn int) responseFunc {
	return func() {
		publichResponse(ch, br, rl, ssn)
	}
}

func publichResponse(ch *amqp.Channel, br *bankutil.BankResponses, rl map[int]int, ssn int) {
	br.Lock()
	defer br.Unlock()
	body, err := json.Marshal(br.Responses[ssn])
	if err != nil {
		log.Println(err)
	} else {
		log.Println("sendt", string(body))
		// bankutil.Publish(ch, body, "RoutingExchange", "result_queue")
	}
	//delete result list ssn
	delete(rl, ssn)
	delete(br.Responses, ssn)
}

func parseLoanResponse(body []byte) (le *bankutil.LoanResponse, err error) {
	le = &bankutil.LoanResponse{}
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
