package routes

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

var (
	ErrKeyNotInContext        = errors.New("key does not exists in context")
	ErrNotAbleToConvertToJson = errors.New("error with json serialization")
)

func bindJson(c *gin.Context, r interface{}) error {
	err := c.BindJSON(&r)
	if err != nil {
		fmt.Println(err)
	}
	return err
}

func saveDatabaseInContext(db *sqlx.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Set example variable
		c.Set("db", db)
		// before request
		c.Next()

	}
}

func getDBfromCtx(c *gin.Context) (*sqlx.DB, bool) {
	sqldb, ok := c.Get("db")
	if !ok {
		log.Error(ErrKeyNotInContext)
	}
	db := sqldb.(*sqlx.DB)
	return db, ok
}

func ToJSON(obj interface{}) string {
	res, err := json.Marshal(obj)
	if err != nil {
		log.Error(err)
	}
	return string(res)
}
