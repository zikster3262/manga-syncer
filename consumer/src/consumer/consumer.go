package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"goquery-client/src/querier"
	"goquery-client/src/utils"
	"time"

	"github.com/zikster3262/shared-lib/page"
	"github.com/zikster3262/shared-lib/rabbitmq"
	"github.com/zikster3262/shared-lib/source"

	"github.com/jmoiron/sqlx"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNotCantRetriveData = errors.New("can't retrive data from database")
	mpChan                = make(chan []page.Page)
	rabbitQueueName       = "pages"
)

type MangaConsumer struct {
	// s3w *s3.Client
	db  *sqlx.DB
	rmq *rabbitmq.RabbitMQClient
}

func NewMangaConsumer(db *sqlx.DB, rmq *rabbitmq.RabbitMQClient) MangaConsumer {
	return MangaConsumer{
		// s3w: s3cliet,
		db:  db,
		rmq: rmq,
	}
}

func (s *MangaConsumer) Sync(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		utils.LogWithInfo("consumer", "consumer is running...")
		msgs, err := s.rmq.Consume(rabbitQueueName)
		utils.FailOnError("rabbitmq", err)

		go parseMsg(msgs, s.db)
		go func() {

			for _, r := range <-mpChan {
				err = s.rmq.PublishMessage("chapters", utils.StructToJson(r))
				if err != nil {
					utils.FailOnError("coordinator", err)
				}
			}
		}()

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}

	}

}

func (s MangaConsumer) Run(ctx context.Context) error {
	g, c := errgroup.WithContext(ctx)
	g.Go(func() error { return s.Sync(c) })
	return g.Wait()
}

func parseMsg(msgs <-chan amqp.Delivery, db *sqlx.DB) {

	cons := make(chan struct{})

	go func(msgs <-chan amqp.Delivery) {
		for d := range msgs {

			m := page.Page{}
			json.Unmarshal([]byte(string(d.Body)), &m)
			mp := source.GetSourceID(db, m.Source_Id)
			ms, _, _ := page.GetPage(db, m.Title)
			pm := querier.ScapeMangaPage(m.Url, mp.Page_Pattern, m.Title, ms.Id, m.Append)
			mpChan <- pm

		}
	}(msgs)

	<-cons
}
