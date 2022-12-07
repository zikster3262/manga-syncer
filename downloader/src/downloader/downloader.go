package downloader

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"goquery-client/src/s3wd"

	"time"

	"github.com/zikster3262/shared-lib/utils"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/zikster3262/shared-lib/img"
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
	rmq *rabbitmq.Client
}

func NewDownloader(db *sqlx.DB, rmq *rabbitmq.Client, c *s3.Client) Downloader {
	return Downloader{
		s3w: c,
		db:  db,
		rmq: rmq,
	}
}

func (s *Downloader) Sync(ctx context.Context) error {
	ticker := time.NewTicker(time.Second * 15)
	defer ticker.Stop()

	pub := s.rmq.CreateChannel()
	err := pub.Qos(1, 0, false)
	if err != nil {
		utils.FailOnCmpError("downloader", "channel queorum", err)
	}
	defer pub.Close()

	for {
		utils.LogWithInfo("Downloader", "Downloader is running...")
		msgs, err := rabbitmq.Consume(pub, "images")
		utils.FailOnError("rabbitmq", err)

		go parseMsg(s.s3w, msgs)

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

func parseMsg(s3w *s3.Client, msgs <-chan amqp.Delivery) {

	cons := make(chan struct{})

	go func(msgs <-chan amqp.Delivery, s3w *s3.Client) {
		for d := range msgs {

			m := img.Image{}
			err := json.Unmarshal([]byte(string(d.Body)), &m)
			if err != nil {
				utils.FailOnCmpError("downloader", "convert-rb-mess-to struct", err)
			}
			c := fmt.Sprintf("%v, %v, %v", m.Title, m.Chapter, m.URL)
			fmt.Println(c)
			i, _ := m.DownloadFile()
			s3wd.UploadFile(s3w, "storage", fmt.Sprintf("%v/%v/%v", m.Title, m.Chapter, m.Filename), i)

		}
	}(msgs, s3w)

	<-cons
}
