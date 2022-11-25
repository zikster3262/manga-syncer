package db

import (
	"fmt"
	"goquery-coordinator/src/utils"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var (
	SQLXConnection *sqlx.DB
	err            error
)

type SQLxProvider struct{}

var onceSQLx sync.Once

func OpenSQLx() *sqlx.DB {

	onceSQLx.Do(func() {
		dsn := os.Getenv("DB_URL")

		fmt.Println(dsn)

		time.Sleep(time.Second * 12)

		SQLXConnection, err = sqlx.Connect("mysql", dsn)
		if err != nil {
			utils.FailOnError("db", err)
		}
	})

	utils.LogWithInfo("connected to database", "db")

	return SQLXConnection
}
