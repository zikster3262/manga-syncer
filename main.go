package main

import (
	"context"
	"goquery-test/src/coordinator"
	"goquery-test/src/db"
	"goquery-test/src/rabbitmq"
	"goquery-test/src/runner"
	"goquery-test/src/utils"
	"net/http"
	"os"
	"syscall"
	"time"

	"goquery-test/src/api"

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
	defer rabbitCh.Close()

	rmq := rabbitmq.CreateRabbitMQClient(rabbitCh, "manga-workers")

	q, err := rmq.CreateRabbitMQueue()
	utils.FailOnError("rabbitmq", err)

	router, err := NewServer(ctx)
	if err != nil {
		utils.LogWithInfo("server", "internal error")
	}

	coordinator := coordinator.NewMangaCoordinator(sqlxDB, rmq, q)

	runners := []runner.Runner{
		runner.NewSignal(os.Interrupt, syscall.SIGTERM),
		coordinator,
		router,
	}

	err = runner.RunParallel(ctx, runners...)
	switch err {
	case context.Canceled, runner.SignalReceived, nil:
	default:
		return err
	}
	return nil
}

func NewServer(ctx context.Context) (runner.Runner, error) {

	rt := api.Register(sqlxDB, ctx)

	return runner.NewServer(
		&http.Server{
			Handler:      rt,
			Addr:         ":8080",
			ReadTimeout:  time.Second * 20,
			WriteTimeout: time.Second * 20,
		}, time.Second*10,
	), nil

}
