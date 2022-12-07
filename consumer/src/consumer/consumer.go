package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/zikster3262/shared-lib/utils"

	"github.com/zikster3262/shared-lib/rabbitmq"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/zikster3262/shared-lib/chapter"
	"github.com/zikster3262/shared-lib/page"
	"github.com/zikster3262/shared-lib/scrape"
	"github.com/zikster3262/shared-lib/source"

	"github.com/jmoiron/sqlx"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNotCantRetriveData = errors.New("can't retrive data from database")
	mpChan                = make(chan []page.SQL)
	rabbitQueueName       = "pages"
)

type Consumer struct {
	s3w *s3.Client
	db  *sqlx.DB
	rmq *rabbitmq.Client
}

func NewConsumer(db *sqlx.DB, rmq *rabbitmq.Client, c *s3.Client) Consumer {
	return Consumer{
		s3w: c,
		db:  db,
		rmq: rmq,
	}
}

func (s *Consumer) Sync(ctx context.Context) error {
	cons := s.rmq.CreateChannel()
	pub := s.rmq.CreateChannel()
	defer cons.Close()
	defer pub.Close()

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		utils.LogWithInfo("consumer-sync", "consumer is running...")
		msgs, err := rabbitmq.Consume(cons, rabbitQueueName)
		utils.FailOnCmpError("rabbitmq", "consume-sync", err)
		for i := 0; i < 5; i++ {
			go parseMsg(msgs, s.db)
		}

		go func() {

			for _, r := range <-mpChan {
				err = rabbitmq.PublishMessage(pub, "chapters", utils.StructToJson(r))
				if err != nil {
					utils.FailOnCmpError("rabbitmq", "publish-sync", err)
				}
			}
		}()

		select {
		case <-ticker.C:
			defer s.rmq.Close()
		case <-ctx.Done():
			return ctx.Err()
		}

	}

}

func (s *Consumer) Consume(ctx context.Context) error {
	sub := s.rmq.CreateChannel()
	pub := s.rmq.CreateChannel()
	defer pub.Close()
	defer sub.Close()

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		utils.LogWithInfo("consumer-consume", "consumer is running...")
		msgs, err := rabbitmq.Consume(sub, "chapters")
		utils.FailOnCmpError("rabbitmq", "consume-consume", err)

		for i := 0; i < 5; i++ {

			go func() {

				for m := range msgs {
					p := page.SQL{}
					json.Unmarshal([]byte(string(m.Body)), &p)

					res, _, _ := chapter.GetChapter(s.db, p.URL)
					if p.URL != res.URL {

						ch := chapter.Chapter{
							PageID:         p.ID,
							Title:          p.Title,
							URL:            p.URL,
							ChapterPattern: p.ChapterPattern,
							Append:         p.Append,
						}
						ch.InsertChapter(s.db)

						imgs := scrape.ScapeChapter(ch)

						for _, i := range imgs {
							// c := fmt.Sprintf("%v, %v, %v", i.Chapter, i.Filename, i.URL)
							// fmt.Println(c)
							err = rabbitmq.PublishMessage(pub, "images", utils.StructToJson(i))
							if err != nil {
								utils.FailOnCmpError("rabbitmq", "publish-consume", err)
							}

						}
					}

				}

			}()

		}

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

			m := page.SQL{}
			json.Unmarshal([]byte(string(d.Body)), &m)
			mp := source.GetSourceID(db, int64(m.SourceID))
			ms, _, _ := page.GetPage(db, m.Title)
			pm := scrape.ScapePage(m, ms.ID, mp.ID)
			mpChan <- pm

		}
	}(msgs)

	<-cons
}
