package main

import (
	"context"
	"goquery-client/src/downloader"
	"goquery-client/src/runner"
	"os"
	"syscall"
	"time"

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

	time.Sleep(time.Second * 10)
	sqlxDB = db.OpenSQLx()

	rbConn := rabbitmq.CreateClient()
	client := storage.CreateNewClient()

	downloader := downloader.NewDownloader(sqlxDB, rbConn, client)

	runners := []runner.Runner{
		runner.NewSignal(os.Interrupt, syscall.SIGTERM),
		downloader,
	}

	err := runner.RunParallel(ctx, runners...)
	switch err {
	case context.Canceled, runner.SignalReceived, nil:
	default:
		return err
	}
	return nil

}
