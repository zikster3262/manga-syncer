package routes

import (
	"errors"
	"goquery-coordinator/src/model"
	"goquery-coordinator/src/utils"
	"net/http"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

var (
	ErrDBDoesNotExists = errors.New("database does not exists")
	ErrRecordExists    = errors.New("record exists in the Database")
)

func healthEndpoint(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"message": "Status OK",
	})
}

func createMangaPage(c *gin.Context) {

	db, ok := getDBfromCtx(c)
	if !ok {
		log.Error(ErrDBDoesNotExists)
	}

	var m model.MangaPage
	err := bindJson(c, &m)
	if err != nil {
		utils.FailOnError("handlers", err)
	}

	// call GetManga func and return SQLManga Struct
	res := model.GetMangaPage(db, m.Manga_URL)

	if res.Manga_URL == m.Manga_URL {

		c.JSON(http.StatusFound, gin.H{"Status": "record exists in the database"})

	} else {

		err = model.InsertMangaPage(db, m)
		if err != nil {
			utils.FailOnError("handlers", err)
			c.JSON(http.StatusInternalServerError, gin.H{"Status": "failed"})
		}

		c.JSON(http.StatusOK, gin.H{"Status": "done"})

	}

}
