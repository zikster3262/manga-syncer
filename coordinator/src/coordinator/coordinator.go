package coordinator

import (
	"context"
	"errors"
	"goquery-coordinator/src/querier"

	"goquery-coordinator/src/utils"
	"time"

	"github.com/zikster3262/shared-lib/page"
	"github.com/zikster3262/shared-lib/rabbitmq"
	"github.com/zikster3262/shared-lib/source"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNotCantRetriveData = errors.New("can't retrive data from database")
	rabbitQueueName       = "pages"
)

type MangaCoordinator struct {
	// s3w *s3.Client
	db  *sqlx.DB
	rmq *rabbitmq.RabbitMQClient
}

func NewMangaCoordinator(db *sqlx.DB, rmq *rabbitmq.RabbitMQClient) MangaCoordinator {
	return MangaCoordinator{
		// s3w: s3cliet,
		db:  db,
		rmq: rmq,
	}
}

func (s *MangaCoordinator) Sync(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		utils.LogWithInfo("coordinator", "coordinator is running... ")
		mgs, err := source.GetAllSources(s.db)
		if err != nil {
			utils.FailOnError("coordinator", err)
		}

		for _, m := range mgs {
			sc := querier.ScapeMangaPage(m)
			InsertManga(ctx, sc, s, m.Id, m.Append)
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

func InsertManga(ctx context.Context, mc []page.Page, s *MangaCoordinator, id int64, appendURL bool) {
	for _, mc := range mc {
		mc.Append = appendURL
		mc.Source_Id = id

		_, ex, _ := page.GetPage(s.db, mc.Title)
		if !ex {
			err := mc.InsertPage(s.db)
			if err != nil {
				utils.FailOnError("coordinator", err)
			}
			err = s.rmq.PublishMessage(rabbitQueueName, utils.StructToJson(mc))
			if err != nil {
				utils.FailOnError("coordinator", err)
			}
		}
	}
}
