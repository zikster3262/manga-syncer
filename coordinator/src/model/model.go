package model

import (
	"database/sql"
	"errors"
	"fmt"
	"goquery-coordinator/src/utils"
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
	Id         int64        `db:"id"`
	Title      string       `db:"title"`
	Url        string       `db:"url"`
	Page_Id    int          `db:"page_id"`
	Date_Added sql.NullTime `db:"date_added"`
	Append     bool         `db:"append"`
}

var (
	mx                 sync.Mutex
	ErrDBInternalError = errors.New("record was not created due to internal error")
)

// GetAllManga func return []MangaSQL sctruct with all results of mangapages database table
func GetAllMangaPages(db *sqlx.DB) (mangas []MangaPageSQL, err error) {

	err = db.Select(&mangas, "SELECT * FROM mangapages;")
	if err != nil {
		utils.FailOnError("db", err)
	}
	return mangas, err

}

// GetManga function takes sqlx DB struct and parameter string
// Return SQLManga result
// If err is not
func GetMangaPage(db *sqlx.DB, p string) (res MangaPageSQL) {
	err := db.Get(&res, fmt.Sprintf("SELECT * FROM mangapages WHERE manga_url = \"%v\"", p))
	if err != nil {
		utils.LogWithInfo("db", "record does not exists in the database")
	}

	return res
}

// InsertManga inserts interface m into mangapages table with sqlx DB struct
// Returns internal DB error on err
func InsertMangaPage(db *sqlx.DB, m interface{}) error {
	_, err := db.NamedExec(`INSERT INTO mangapages (manga_url, home_pattern, page_pattern, append) VALUES (:manga_url, :home_pattern, :page_pattern, :append);`, m)
	if err != nil {
		utils.FailOnError("db", ErrDBInternalError)
	}
	return err
}

func (m Manga) InsertManga(db *sqlx.DB) error {
	mx.Lock()
	_, err := db.NamedExec(`INSERT INTO manga (title, url, page_id, append)  VALUES (:title, :url, (select id from db.mangapages WHERE id = :page_id), :append);`, m)
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
