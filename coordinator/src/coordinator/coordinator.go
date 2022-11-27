package coordinator

import (
	"context"
	"errors"
	"goquery-coordinator/src/model"
	"goquery-coordinator/src/querier"
	"goquery-coordinator/src/rabbitmq"
	"goquery-coordinator/src/utils"
	"time"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNotCantRetriveData = errors.New("can't retrive data from database")
	qName                 = "manga-workers"
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
		mgs, err := model.GetAllMangaPages(s.db)
		if err != nil {
			utils.FailOnError("coordinator", err)
		}

		for _, m := range mgs {
			sc := querier.ScapeMangaPage(m)
			InsertManga(sc, s, m.Id, m.Append)
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

func InsertManga(mc []model.Manga, s *MangaCoordinator, id int64, appendURL bool) {
	for _, mc := range mc {
		mc.Append = appendURL
		mc.Page_Id = id

		_, ex, _ := model.GetManga(s.db, mc.Title)
		if !ex {
			err := mc.InsertManga(s.db)
			if err != nil {
				utils.FailOnError("coordinator", err)
			}
			err = s.rmq.PublishMessage(qName, utils.StructToJson(mc))
			if err != nil {
				utils.FailOnError("coordinator", err)
			}
		}
	}
}

// func (s *MangaCoordinator) GetS3Object(uuid string) bool {

// 	listObjsResponse, err := s.s3w.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
// 		Bucket: aws.String("requests"),
// 	})

// 	if err != nil {
// 		utils.FailOnError("coordinator", err)
// 	}

// 	for _, object := range listObjsResponse.Contents {
// 		if *object.Key == uuid {
// 			return true
// 		}
// 	}

// 	return false

// }

// func (s *MangaCoordinator) PutS3Object(m model.DbRequest) error {
// 	bt := utils.StructToJson(m)
// 	body := bytes.NewReader(bt)

// 	_, err := s.s3w.PutObject(context.TODO(), &s3.PutObjectInput{
// 		Bucket: aws.String("requests"),
// 		Key:    aws.String(m.Uuid),
// 		Body:   body,
// 	})

// 	if err != nil {
// 		utils.FailOnError(err, "Couldn't upload file: "+err.Error())
// 	}
// 	return err

// }
