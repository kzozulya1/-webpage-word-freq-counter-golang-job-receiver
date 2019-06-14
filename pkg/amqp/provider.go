package amqp

import (
	logger "app/pkg/loggerutil"
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

// const (
// 	chType = "direct"
// )

//Rabbit Client data
type RabbitMqClient struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	exchange string
	key      string
}

//Rabbit Client interface
//Create connection, channel and declare a queue for send URL parse jobs
func (r *RabbitMqClient) Init(dialURL, exchange, routeKey string) {
	var err error
	r.conn, err = amqp.Dial(dialURL)
	if err != nil {
		r.failOnError(err, "Can't dial "+dialURL)
	}

	//Create a channel
	r.ch, err = r.conn.Channel()
	if err != nil {
		r.failOnError(err, "Can't get channel")
	}

	err = r.ch.ExchangeDeclare(exchange, //exchange point name
		amqp.ExchangeDirect, //kind
		false,               //durable
		false,               //auto-delete  - deleted if no binded queues
		false,               //internal, not accept accept publishings
		false,               //noWait When true, declare without waiting for a confirmation from the server
		nil)
	if err != nil {
		r.failOnError(err, "Can't declare excange")
	}

	//Assign exchange point name
	r.exchange = exchange
	//Assign route key for publishing into exchange point
	r.key = routeKey
}

//Publish a job into queue
func (r *RabbitMqClient) PublishJson(jsonData []byte) {
	// Prepare message
	msg := amqp.Publishing{
		DeliveryMode: amqp.Transient, /*amqp.Persistent,*/
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		Body:         jsonData,
	}

	// This is a mandatory delivery, so it will not be dropped if there are no
	// queues bound to the logs exchange.
	err := r.ch.Publish(r.exchange, //exchange name
		r.key,  //routing key
		false,  //mandatory
		false,  //immediate
		msg)
	if err != nil {
		// Since publish is asynchronous this can happen if the network connection
		// is reset or if the server has run out of resources.
		r.failOnError(err, "Can't publish msg")
	}
}

//Fail over routine -
func (r *RabbitMqClient) failOnError(err error, msg string) {
	if err != nil {
		logger.Log(fmt.Sprintf("%s: %s", msg, err), "error.log")
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

//Rabbit Client interface
func (r *RabbitMqClient) Destruct() {
	// Close Channel
	r.ch.Close()

	// Close Connection
	r.conn.Close()
}
