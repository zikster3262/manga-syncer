package rabbitmq

import (
	"goquery-coordinator/src/utils"
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
	ch *amqp.Channel
}

func CreateRabbitMQClient(r *amqp.Channel) *RabbitMQClient {
	return &RabbitMQClient{
		ch: r,
	}
}

func (rmq *RabbitMQClient) PublishMessage(name string, body []byte) error {

	err := rmq.ch.Publish(
		"",    // exchange
		name,  // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent,
		})
	utils.FailOnError("rabbitmq", err)
	return err
}
