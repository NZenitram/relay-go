package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	// _ "github.com/lib/pq"
	_ "github.com/go-sql-driver/mysql"
)

var (
	db   *sql.DB
	once sync.Once
)

// InitDB initializes the database connection
func InitDB() {
	once.Do(func() {
		var err error
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
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			log.Fatalf("Error opening database connection: %v", err)
		}

		// Ping the database to verify the connection
		err = db.Ping()
		if err != nil {
			log.Fatalf("Error pinging database: %v", err)
		}

		log.Println("Successfully connected to the MySQL database")
	})
}

// // InitDB initializes the database connection
// func InitDB() {
// 	once.Do(func() {
// 		var err error
// 		dbHost := os.Getenv("POSTGRES_HOST")
// 		dbPort := os.Getenv("POSTGRES_PORT")
// 		dbUser := os.Getenv("POSTGRES_USER")
// 		dbPassword := os.Getenv("POSTGRES_PASSWORD")
// 		dbName := os.Getenv("POSTGRES_DB")
// 		sslMode := os.Getenv("POSTGRES_SSL_MODE")

// 		connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
// 			dbHost, dbPort, dbUser, dbPassword, dbName, sslMode)

// 		db, err = sql.Open("postgres", connStr)
// 		if err != nil {
// 			log.Fatalf("Error opening database connection: %v", err)
// 		}

// 		err = db.Ping()
// 		if err != nil {
// 			log.Fatalf("Error pinging database: %v", err)
// 		}

// 		log.Println("Successfully connected to the database")
// 	})
// }

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
