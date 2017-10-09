package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/cborum/go-loan-broker/bankutil"
	"github.com/streadway/amqp"
)

func rpc() (res *bankutil.LoanResponse, err error) {
	conn, err := amqp.Dial("amqp://guest:guest@datdb.cphbusiness.dk")
	// conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	exchangeName := "cphbusiness.bankJSON"
	// err = ch.ExchangeDeclare(exchangeName, "fanout", false, false, false, false, nil)
	// bankutil.FailOnError(err, "Failed to declare an exchange")

	q, err := bankutil.StdQueueDeclare(ch, "rpc_out")
	bankutil.FailOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(q.Name, "", true, true, false, false, nil)
	bankutil.FailOnError(err, "Failed to register a consumer")

	lr := &bankutil.LoanRequest{
		Ssn:          653412342,
		CreditScore:  650,
		LoanAmount:   4234.54,
		LoanDuration: 1,
	}
	body, err := json.Marshal(lr)

	err = bankutil.PublishWithReply(ch, body, exchangeName, "", q.Name)
	bankutil.FailOnError(err, "Failed to publish a message")

	for d := range msgs {
		res = &bankutil.LoanResponse{}
		err = json.Unmarshal(d.Body, res)
		if err == nil && res.InterestRate != 0 {
			bankutil.FailOnError(err, "Failed to convert body to integer")
			break
		} else if err != nil {
			log.Println(err)
		}
	}

	return
}

// func main() {
// 	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
// 	rand.Seed(time.Now().UTC().UnixNano())

// 	log.Println(" [x] Requesting")
// 	res, err := rpc()
// 	bankutil.FailOnError(err, "Failed to handle RPC request")

// 	log.Printf(" [.] Got %#v", res)
// }

const (
	bankExchange  = "cphbusiness.bankJSON"
	routeExchange = "LB4.RouteExchange"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Ltime)
	log.Println(" [x] Requesting")

	// conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	conn, err := amqp.Dial("amqp://guest:guest@datdb.cphbusiness.dk")
	bankutil.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	bankutil.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = bankutil.StdExchangeDeclare(ch, routeExchange)
	bankutil.FailOnError(err, "Failed to declare exchange")

	rpcQueue, err := bankutil.StdQueueDeclare(ch, "rpc_out")
	bankutil.FailOnError(err, "Failed to declare queue")

	routeQueue, err := bankutil.StdQueueDeclareWithBind(ch, "json_bank_in", routeExchange)
	bankutil.FailOnError(err, "Failed to declare queue")

	_, err = bankutil.StdQueueDeclareWithBind(ch, "json_bank_out", routeExchange)
	bankutil.FailOnError(err, "Failed to declare queue")

	inMsgs, err := ch.Consume(routeQueue.Name, "", true, false, false, false, nil)
	bankutil.FailOnError(err, "Failed to register a consumer")

	for m := range inMsgs {
		log.Println(string(m.Body))
		le, err := handleInMsg(m.Body, rpcQueue, ch)
		if err != nil {
			log.Println(err)
		}
		log.Println(le)
		err = publishLoanResponse(le, ch)
		if err != nil {
			log.Println(err)
		}
	}
}

func handleInMsg(body []byte, rpcQueue amqp.Queue, ch *amqp.Channel) (le *bankutil.LoanResponse, err error) {
	lr := &bankutil.LoanRequest{}
	err = json.Unmarshal(body, lr)
	if err != nil {
		return nil, err
	}

	jsonBody, err := json.Marshal(lr)

	err = bankutil.PublishWithReply(ch, jsonBody, bankExchange, "", rpcQueue.Name)
	if err != nil {
		return nil, err
	}

	bankMsgs, err := ch.Consume(rpcQueue.Name, "abc", true, false, false, false, nil)
	if err != nil {
		return nil, err
	}

	for d := range bankMsgs {
		log.Println(http.DetectContentType(d.Body))
		le = &bankutil.LoanResponse{}
		err = json.Unmarshal(d.Body, le)
		if err != nil {
			return nil, err
		}
		break
	}
	ch.Cancel("abc", false)

	return le, err
}

func publishLoanResponse(le *bankutil.LoanResponse, ch *amqp.Channel) error {
	if le == nil {
		return errors.New("nil LoanResponse")
	}

	jsonBody, err := json.Marshal(le)
	if err != nil {
		return err
	}

	err = bankutil.Publish(ch, jsonBody, routeExchange, "json_bank_out")
	return err
}
