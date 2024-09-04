package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	_ "github.com/lib/pq"
)

var (
	db   *sql.DB
	once sync.Once
)

// InitDB initializes the database connection
func InitDB() {
	once.Do(func() {
		var err error
		dbHost := os.Getenv("POSTGRES_HOST")
		dbPort := os.Getenv("POSTGRES_PORT")
		dbUser := os.Getenv("POSTGRES_USER")
		dbPassword := os.Getenv("POSTGRES_PASSWORD")
		dbName := os.Getenv("POSTGRES_DB")

		connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			dbHost, dbPort, dbUser, dbPassword, dbName)

		db, err = sql.Open("postgres", connStr)
		if err != nil {
			log.Fatalf("Error opening database connection: %v", err)
		}

		err = db.Ping()
		if err != nil {
			log.Fatalf("Error pinging database: %v", err)
		}

		log.Println("Successfully connected to the database")
	})
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
