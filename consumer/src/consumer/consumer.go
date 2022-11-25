package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"goquery-client/src/model"
	"goquery-client/src/querier"
	"goquery-client/src/rabbitmq"
	"goquery-client/src/utils"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/streadway/amqp"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNotCantRetriveData = errors.New("can't retrive data from database")
	mpChan                = make(chan []model.Manga)
	rq                    = amqp.Queue{Name: "workers"}
)

type MangaConsumer struct {
	// s3w *s3.Client
	db  *sqlx.DB
	rmq *rabbitmq.RabbitMQClient
	q   amqp.Queue
}

func NewMangaConsumer(db *sqlx.DB, rmq *rabbitmq.RabbitMQClient, q amqp.Queue) MangaConsumer {
	return MangaConsumer{
		// s3w: s3cliet,
		db:  db,
		rmq: rmq,
		q:   q,
	}
}

func (s *MangaConsumer) Sync(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	var wg sync.WaitGroup

	for {
		utils.LogWithInfo("consumer", "consumer is running...")
		msgs, err := s.rmq.Consume(s.q)
		utils.FailOnError("rabbitmq", err)

		wg.Add(1)

		go parseMsg(msgs, s.db)
		go func() {
			defer wg.Done()
			for _, r := range <-mpChan {
				err = s.rmq.PublishMessage(rq, utils.StructToJson(r))
				if err != nil {
					utils.FailOnError("coordinator", err)
				}
			}
		}()

		wg.Wait()

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

			m := model.Manga{}
			json.Unmarshal([]byte(string(d.Body)), &m)
			mp := model.GetMangaPageID(db, m.Page_Id)
			ms, _, _ := model.GetManga(db, m.Title)
			pm := querier.ScapeMangaPage(m.Url, mp.Page_Pattern, m.Title, ms.Id, m.Append)
			mpChan <- pm

		}
	}(msgs)

	<-cons
}
