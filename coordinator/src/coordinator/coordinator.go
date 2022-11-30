package coordinator

import (
	"context"
	"errors"

	"goquery-coordinator/src/utils"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/zikster3262/shared-lib/page"
	"github.com/zikster3262/shared-lib/rabbitmq"
	"github.com/zikster3262/shared-lib/scrape"
	"github.com/zikster3262/shared-lib/source"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNotCantRetriveData = errors.New("can't retrive data from database")
	rabbitQueueName       = "pages"
)

type MangaCoordinator struct {
	db  *sqlx.DB
	rmq *rabbitmq.RabbitMQClient
}

func NewMangaCoordinator(db *sqlx.DB, rmq *rabbitmq.RabbitMQClient) MangaCoordinator {
	return MangaCoordinator{
		db:  db,
		rmq: rmq,
	}
}

func (s *MangaCoordinator) Sync(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	pub := s.rmq.CreateChannel()
	defer pub.Close()

	for {
		utils.LogWithInfo("coordinator", "coordinator is running... ")
		mgs, err := source.GetAllSources(s.db)
		if err != nil {
			utils.FailOnError("coordinator", err)
		}

		for _, m := range mgs {
			sc := scrape.ScapeSource(m)
			InsertManga(sc, s, pub)
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s MangaCoordinator) Run(ctx context.Context) error {
	g, c := errgroup.WithContext(ctx)
	g.Go(func() error { return s.Sync(c) })
	return g.Wait()
}

func InsertManga(mc []page.Page, s *MangaCoordinator, channel *amqp091.Channel) {
	for _, mc := range mc {
		_, ex, _ := page.GetPage(s.db, mc.Title)
		if !ex {
			err := mc.InsertPage(s.db)
			if err != nil {
				utils.FailOnError("coordinator", err)
			}
		}

		err := rabbitmq.PublishMessage(channel, rabbitQueueName, utils.StructToJson(mc))
		if err != nil {
			utils.FailOnError("coordinator", err)
		}
	}
}
