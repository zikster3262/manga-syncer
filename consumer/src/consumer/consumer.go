package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"goquery-client/src/querier"
	"goquery-client/src/utils"
	"time"

	"github.com/zikster3262/shared-lib/chapter"
	"github.com/zikster3262/shared-lib/page"
	"github.com/zikster3262/shared-lib/rabbitmq"
	"github.com/zikster3262/shared-lib/source"

	"github.com/jmoiron/sqlx"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNotCantRetriveData = errors.New("can't retrive data from database")
	mpChan                = make(chan []page.PageSQL)
	rabbitQueueName       = "pages"
)

type Consumer struct {
	// s3w *s3.Client
	db  *sqlx.DB
	rmq *rabbitmq.RabbitMQClient
}

func NewConsumer(db *sqlx.DB, rmq *rabbitmq.RabbitMQClient) Consumer {
	return Consumer{
		// s3w: s3cliet,
		db:  db,
		rmq: rmq,
	}
}

func (s *Consumer) Sync(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 1)
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

func (s *Consumer) Consume(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	for {
		utils.LogWithInfo("consumer", "consumer is running...")
		msgs, err := s.rmq.Consume("chapters")
		utils.FailOnError("rabbitmq", err)

		go func() {

			for m := range msgs {
				p := page.PageSQL{}
				json.Unmarshal([]byte(string(m.Body)), &p)

				res, _, _ := chapter.GetChapter(s.db, p.Url)
				if p.Url != res.Url {
					ch := chapter.CreateNewChapter(p.Id, p.Title, p.Url, p.Append)
					ch.InsertChapter(s.db)
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

func (s Consumer) Run(ctx context.Context) error {
	g, c := errgroup.WithContext(ctx)
	g.Go(func() error { return s.Sync(c) })
	g.Go(func() error { return s.Consume(c) })
	return g.Wait()
}

func parseMsg(msgs <-chan amqp.Delivery, db *sqlx.DB) {

	cons := make(chan struct{})

	go func(msgs <-chan amqp.Delivery) {
		for d := range msgs {

			m := page.PageSQL{}
			json.Unmarshal([]byte(string(d.Body)), &m)
			mp := source.GetSourceID(db, int64(m.Source_Id))
			ms, _, _ := page.GetPage(db, m.Title)
			pm := querier.ScapeMangaPage(m.Url, mp.Manga_URL, mp.Page_Pattern, m.Title, ms.Id, mp.Id, m.Append)
			mpChan <- pm

		}
	}(msgs)

	<-cons
}
