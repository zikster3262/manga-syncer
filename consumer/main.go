package main

import (
	"context"
	"goquery-client/src/consumer"
	"goquery-client/src/runner"
	"os"
	"syscall"

	"github.com/zikster3262/shared-lib/rabbitmq"

	"github.com/zikster3262/shared-lib/db"
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
	rbConn := rabbitmq.CreateClient()
	client := storage.CreateNewClient()

	consumer := consumer.NewConsumer(sqlxDB, rbConn, client)

	runners := []runner.Runner{
		runner.NewSignal(os.Interrupt, syscall.SIGTERM),
		consumer,
	}

	err := runner.RunParallel(ctx, runners...)
	switch err {
	case context.Canceled, runner.SignalReceived, nil:
	default:
		return err
	}
	return nil
}
