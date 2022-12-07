package main

import (
	"context"
	"goquery-coordinator/src/coordinator"
	"goquery-coordinator/src/runner"
	"goquery-coordinator/src/utils"
	"net/http"
	"os"
	"syscall"
	"time"

	"goquery-coordinator/src/api"

	"github.com/jmoiron/sqlx"
	"github.com/zikster3262/shared-lib/db"
	"github.com/zikster3262/shared-lib/rabbitmq"
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
	router, err := NewServer(ctx)
	if err != nil {
		utils.LogWithInfo("server", "internal error")
	}

	coordinator := coordinator.NewMangaCoordinator(sqlxDB, rbConn)

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
