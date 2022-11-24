package coordinator

import (
	"context"
	"errors"
	"goquery-test/src/model"
	"goquery-test/src/querier"
	"goquery-test/src/utils"
	"time"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNotCantRetriveData = errors.New("can't retrive data from database")
)

type MangaCoordinator struct {
	// s3w *s3.Client
	db *sqlx.DB
	// rmq *rabbitmq.RabbitMQClient
	// q   amqp.Queue
}

func NewMangaCoordinator(db *sqlx.DB) MangaCoordinator {
	return MangaCoordinator{
		// s3w: s3cliet,
		db: db,
		// rmq: rmq,
		// q:   q,
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

		IterateViaMangaPages(mgs, s.db)

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

func IterateViaMangaPages(mgs []model.MangaPageSQL, db *sqlx.DB) {
	for _, m := range mgs {
		sc := querier.ScapeMangaPage(m)
		IterateViaManga(sc, db)
	}
}

func IterateViaManga(mc []model.Manga, db *sqlx.DB) {
	for _, mc := range mc {
		_, ex := model.GetManga(db, mc.Title)
		if !ex {
			_ = mc.InsertManga(db)
		} else {
			utils.LogWithInfo("coordinator", "record exists in manga table")
		}

	}

}
