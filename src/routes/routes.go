package routes

import (
	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
)

func Routes(router *gin.Engine, db *sqlx.DB) {

	group := router.Group("/api/v1")
	group.Use(saveDatabaseInContext(db))

	group.GET("/health", healthEndpoint)

	group.POST("/mng", createMangaPage)

}
