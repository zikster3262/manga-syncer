package rabbitmq

import (
	"goquery-client/src/utils"
	"os"

	"github.com/streadway/amqp"
)

func ConnectToRabbit() (*amqp.Channel, error) {
	conn, err := amqp.Dial(os.Getenv("RABBITMQ_ADDRESS"))
	utils.FailOnError("rabbitmq", err)

	ch, err := conn.Channel()
	utils.FailOnError("rabbitmq", err)

	utils.LogWithInfo("connected to rabbitMQ", "rabbitmq")
	return ch, err
}

type RabbitMQClient struct {
	ch   *amqp.Channel
	name string
}

func CreateRabbitMQClient(r *amqp.Channel, name string) *RabbitMQClient {
	return &RabbitMQClient{
		ch:   r,
		name: name,
	}
}

func (rmq *RabbitMQClient) CreateRabbitMQueue() (amqp.Queue, error) {
	q, err := rmq.ch.QueueDeclare(
		rmq.name, // name
		true,     // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	utils.FailOnError("rabbitmq", err)
	return q, err
}

func (rmq *RabbitMQClient) PublishMessage(q amqp.Queue, body []byte) error {

	err := rmq.ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		})
	utils.FailOnError("rabbitmq", err)
	return err
}

func (rmq *RabbitMQClient) Consume(q amqp.Queue) (<-chan amqp.Delivery, error) {

	msgs, err := rmq.ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)

	return msgs, err
}
