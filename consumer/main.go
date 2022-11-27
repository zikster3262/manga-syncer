package main

import (
	"context"
	"goquery-client/src/consumer"
	"goquery-client/src/db"
	"goquery-client/src/runner"
	"goquery-client/src/utils"
	"os"
	"syscall"
	"time"

	"github.com/zikster3262/shared-lib/rabbitmq"

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

	time.Sleep(time.Second * 12)

	sqlxDB = db.OpenSQLx()

	rabbitCh, err := rabbitmq.ConnectToRabbit()
	utils.FailOnError("rabbitmq", err)
	defer rabbitCh.Close()

	rmq := rabbitmq.CreateRabbitMQClient(rabbitCh)

	coordinator := consumer.NewMangaConsumer(sqlxDB, rmq)

	runners := []runner.Runner{
		runner.NewSignal(os.Interrupt, syscall.SIGTERM),
		coordinator,
	}

	err = runner.RunParallel(ctx, runners...)
	switch err {
	case context.Canceled, runner.SignalReceived, nil:
	default:
		return err
	}
	return nil
}
