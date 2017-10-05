package bankutil

import (
	"log"

	"github.com/streadway/amqp"
)

// LoanRequest ...
type LoanRequest struct {
	Ssn          int     `xml:"ssn" json:"ssn"`
	CreditScore  int     `xml:"creditScore" json:"creditScore"`
	LoanAmount   float64 `xml:"loanAmount" json:"loanAmount"`
	LoanDuration int     `xml:"loanDuration" json:"loanDuration"`
}

// LoanResponse ...
type LoanResponse struct {
	InterestRate float64 `xml:"interestRate" json:"interestRate"`
	Ssn          int     `xml:"ssn" json:"ssn"`
}

// Publish ...
func Publish(ch *amqp.Channel, body []byte, exchangeName string, route string) error {
	return ch.Publish(
		exchangeName, // exchange
		route,        // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			// ContentType: "text/json",
			Body: body,
		})
}

// PublishWithReply ...
func PublishWithReply(ch *amqp.Channel, body []byte, exchangeName string, route string, replyQueueName string) error {
	return ch.Publish(
		exchangeName, // exchange
		route,        // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			// ContentType: "text/json",
			ReplyTo:     replyQueueName,
			Body:        body,
		})
}

// StdQueueDeclare ...
func StdQueueDeclare(ch *amqp.Channel, queueName string) (amqp.Queue, error) {
	return ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
}

// StdQueueDeclareWithBind ...
func StdQueueDeclareWithBind(ch *amqp.Channel, queueName string, exchangeName string) (amqp.Queue, error) {
	q, err := StdQueueDeclare(ch, queueName)
	if err != nil {
		return q, err
	}

	return q, ch.QueueBind(
		q.Name,       // queue bind name
		q.Name,       // queue routing key
		exchangeName, // exchange
		false,        // no wait
		nil,          // table
	)
}

// FailOnError ...
func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// StdExchangeDeclare ...
func StdExchangeDeclare(ch *amqp.Channel, exchangeName string) error {
	return ch.ExchangeDeclare(
		exchangeName, // name
		"direct",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
}
