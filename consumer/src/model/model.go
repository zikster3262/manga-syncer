package model

import (
	"database/sql"
	"errors"
	"fmt"
	"goquery-client/src/utils"
	"sync"

	"github.com/jmoiron/sqlx"
)

type MangaPage struct {
	Manga_URL    string `json:"manga_url"`
	Home_Pattern string `json:"home_pattern"`
	Page_Pattern string `json:"page_pattern"`
	Append       bool   `json:"append"`
}

type MangaPageSQL struct {
	Id           int64        `db:"id"`
	Manga_URL    string       `db:"manga_url"`
	Home_Pattern string       `db:"home_pattern"`
	Page_Pattern string       `db:"page_pattern"`
	Date_Added   sql.NullTime `db:"date_added"`
	Append       bool         `db:"append"`
}

type Manga struct {
	Title   string `json:"title"`
	Url     string `json:"url"`
	Page_Id int64  `json:"page_id"`
	Append  bool   `json:"append"`
}

type MangaSQL struct {
	Id         int64         `db:"id"`
	Title      string        `db:"title"`
	Url        string        `db:"url"`
	Page_Id    sql.NullInt64 `db:"page_id"`
	Date_Added sql.NullTime  `db:"date_added"`
	Append     sql.NullBool  `db:"append"`
}

var (
	mx                 sync.Mutex
	ErrDBInternalError = errors.New("record was not created due to internal error")
)

func GetMangaPageID(db *sqlx.DB, id int64) (res MangaPageSQL) {
	mx.Lock()
	err := db.Get(&res, fmt.Sprintf("SELECT * FROM mangapages WHERE id = %v;", id))
	if err != nil {
		utils.LogWithInfo("db", "record does not exists in the database")
	}

	mx.Unlock()

	return res
}

func (m Manga) InsertToMangaPage(db *sqlx.DB) error {
	mx.Lock()
	_, err := db.NamedExec(`INSERT INTO mangapage (title, url, manga_id, append)  VALUES (:title, :url, (select id from db.manga WHERE id = :page_id), :append);`, m)
	if err != nil {
		utils.FailOnError("coordinator", ErrDBInternalError)
	}
	mx.Unlock()
	return err
}

func GetManga(db *sqlx.DB, p string) (MangaSQL, bool, error) {
	mx.Lock()
	var res MangaSQL
	err := db.Get(&res, fmt.Sprintf("SELECT * FROM manga WHERE title = \"%v\"", p))
	mx.Unlock()
	if err != nil {
		return MangaSQL{}, false, err
	}
	return res, true, err
}
