package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var (
	db   *sql.DB
	once sync.Once
)

// InitDB initializes the database connection
func InitDB() error {
	var initErr error
	once.Do(func() {
		dbHost := os.Getenv("MYSQL_HOST")
		dbPort := os.Getenv("MYSQL_PORT")
		dbUser := os.Getenv("MYSQL_USER")
		dbPassword := os.Getenv("MYSQL_PASSWORD")
		dbName := os.Getenv("MYSQL_DB")
		sslMode := os.Getenv("MYSQL_SSL_MODE")

		// Construct the DSN (Data Source Name)
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			dbUser, dbPassword, dbHost, dbPort, dbName)

		// Handle SSL mode
		if sslMode == "disable" {
			dsn += "&tls=false"
		} else if sslMode != "" {
			dsn += "&tls=true"
		}

		// Open the database connection
		db, initErr = sql.Open("mysql", dsn)
		if initErr != nil {
			return
		}

		// Ping the database to verify the connection
		initErr = db.Ping()
		if initErr != nil {
			return
		}

		log.Println("Successfully connected to the MySQL database")
	})
	return initErr
}

// GetDB returns the database connection
func GetDB() *sql.DB {
	if db == nil {
		log.Fatal("Database connection has not been initialized. Call InitDB() first.")
	}
	return db
}

// CloseDB closes the database connection
func CloseDB() {
	if db != nil {
		db.Close()
		log.Println("Database connection closed")
	}
}
