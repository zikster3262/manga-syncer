package api

import (
	"context"
	"goquery-coordinator/src/routes"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

func Register(db *sqlx.DB, ctx context.Context) *gin.Engine {

	router := gin.New()
	routes.Routes(router, db)

	return router
}
