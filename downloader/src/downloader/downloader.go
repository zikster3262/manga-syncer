package downloader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"goquery-client/src/utils"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/zikster3262/shared-lib/chapter"
	"github.com/zikster3262/shared-lib/rabbitmq"

	"github.com/jmoiron/sqlx"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNotCantRetriveData = errors.New("can't retrive data from database")
	// mpChan                = make(chan []page.PageSQL)
)

type Downloader struct {
	s3w *s3.Client
	db  *sqlx.DB
	rmq *rabbitmq.RabbitMQClient
}

func NewDownloader(db *sqlx.DB, rmq *rabbitmq.RabbitMQClient, c *s3.Client) Downloader {
	return Downloader{
		s3w: c,
		db:  db,
		rmq: rmq,
	}
}

func (s *Downloader) Sync(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()

	pub := s.rmq.CreateChannel()
	defer pub.Close()

	for {
		utils.LogWithInfo("Downloader", "Downloader is running...")
		msgs, err := rabbitmq.Consume(pub, "images")
		utils.FailOnError("rabbitmq", err)

		go parseMsg(msgs)
		// go func() {

		// 	for _, r := range <-mpChan {
		// 		err = s.rmq.PublishMessage("chapters", utils.StructToJson(r))
		// 		if err != nil {
		// 			utils.FailOnError("coordinator", err)
		// 		}
		// 	}
		// }()

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}

	}

}

func (s Downloader) Run(ctx context.Context) error {
	g, c := errgroup.WithContext(ctx)
	g.Go(func() error { return s.Sync(c) })
	return g.Wait()
}

func parseMsg(msgs <-chan amqp.Delivery) {

	cons := make(chan struct{})

	go func(msgs <-chan amqp.Delivery) {
		for d := range msgs {

			m := chapter.Chapter{}
			json.Unmarshal([]byte(string(d.Body)), &m)
			fmt.Println(m)

		}
	}(msgs)

	<-cons
}
