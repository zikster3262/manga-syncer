package main

import (
	"context"
	"goquery-client/src/consumer"
	"goquery-client/src/runner"
	"goquery-client/src/utils"
	"os"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/zikster3262/shared-lib/db"
	"github.com/zikster3262/shared-lib/rabbitmq"
	"github.com/zikster3262/shared-lib/storage"

	"github.com/jmoiron/sqlx"
)

var (
	sqlxDB *sqlx.DB
)

func main() {
	Initialize()
}

func Initialize() error {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sqlxDB = db.OpenSQLx()

	rabbitCh, err := rabbitmq.ConnectToRabbit()
	utils.FailOnError("rabbitmq", err)

	confirms := make(chan amqp.Confirmation)
	rabbitCh.NotifyPublish(confirms)
	go func() {
		for confirm := range confirms {
			if !confirm.Ack {
				utils.LogWithInfo("rabbitmq", "Failed")
			}
		}
	}()

	err = rabbitCh.Confirm(false)
	utils.FailOnError("rabbitmq", err)

	defer rabbitCh.Close()

	client := storage.CreateNewClient()
	rmq := rabbitmq.CreateRabbitMQClient(rabbitCh)

	consumer := consumer.NewConsumer(sqlxDB, rmq, client)

	runners := []runner.Runner{
		runner.NewSignal(os.Interrupt, syscall.SIGTERM),
		consumer,
	}

	err = runner.RunParallel(ctx, runners...)
	switch err {
	case context.Canceled, runner.SignalReceived, nil:
	default:
		return err
	}
	return nil
}
