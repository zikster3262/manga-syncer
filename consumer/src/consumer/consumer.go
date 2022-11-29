package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"goquery-client/src/utils"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/zikster3262/shared-lib/chapter"
	"github.com/zikster3262/shared-lib/page"
	"github.com/zikster3262/shared-lib/rabbitmq"
	"github.com/zikster3262/shared-lib/scrape"
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
	s3w *s3.Client
	db  *sqlx.DB
	rmq *rabbitmq.RabbitMQClient
}

func NewConsumer(db *sqlx.DB, rmq *rabbitmq.RabbitMQClient, c *s3.Client) Consumer {
	return Consumer{
		s3w: c,
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
	ticker := time.NewTicker(time.Second * 2)
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

					ch := chapter.Chapter{
						Page_id:         p.Id,
						Title:           p.Title,
						Url:             p.Url,
						Chapter_Pattern: p.Chapter_Pattern,
						Append:          p.Append,
					}
					ch.InsertChapter(s.db)

					imgs := scrape.ScapeChapter(ch)

					for _, i := range imgs {
						err = s.rmq.PublishMessage("images", utils.StructToJson(i))
						if err != nil {
							utils.FailOnError("coordinator", err)
						}

					}
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
			pm := scrape.ScapePage(m, ms.Id, mp.Id)
			mpChan <- pm

		}
	}(msgs)

	<-cons
}
