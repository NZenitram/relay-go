package database

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	redisClient *redis.Client
	redisCtx    = context.Background()
)

// InitRedis initializes the Redis connection
func InitRedis() error {
	redisAddr := os.Getenv("REDIS_HOST")
	if redisAddr == "" {
		return fmt.Errorf("REDIS_HOST environment variable is not set")
	}

	redisPassword := os.Getenv("REDIS_PASSWORD")
	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPassword, // Will be empty string if not set
		DB:       0,
	})

	// Test the connection
	_, err := redisClient.Ping(redisCtx).Result()
	if err != nil {
		if err.Error() == "ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?" {
			// If we get this specific error, try reconnecting without a password
			redisClient = redis.NewClient(&redis.Options{
				Addr: redisAddr,
				DB:   0,
			})
			_, err = redisClient.Ping(redisCtx).Result()
		}
		if err != nil {
			return fmt.Errorf("failed to connect to Redis: %v", err)
		}
	}

	log.Println("Successfully connected to Redis")
	return nil
}

// GetRedisClient returns the Redis client
func GetRedisClient() *redis.Client {
	if redisClient == nil {
		log.Fatal("Redis client has not been initialized. Call InitRedis() first.")
	}
	return redisClient
}

// CloseRedis closes the Redis connection
func CloseRedis() {
	if redisClient != nil {
		redisClient.Close()
		log.Println("Redis connection closed")
	}
}

// CacheUserData caches user data in Redis
func CacheUserData(userID string, userData interface{}) error {
	key := fmt.Sprintf("user:%s", userID)
	data, err := json.Marshal(userData)
	if err != nil {
		return fmt.Errorf("failed to marshal user data: %v", err)
	}

	// Cache for 1 hour
	err = redisClient.Set(redisCtx, key, data, time.Hour).Err()
	if err != nil {
		return fmt.Errorf("failed to cache user data: %v", err)
	}

	return nil
}

// GetCachedUserData retrieves user data from Redis cache
func GetCachedUserData(userID string, userData interface{}) (bool, error) {
	key := fmt.Sprintf("user:%s", userID)
	data, err := redisClient.Get(redisCtx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil // Cache miss
		}
		return false, fmt.Errorf("failed to get cached user data: %v", err)
	}

	err = json.Unmarshal([]byte(data), userData)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal cached user data: %v", err)
	}

	return true, nil // Cache hit
}

// InvalidateUserCache removes user data from Redis cache
func InvalidateUserCache(userID string) error {
	key := fmt.Sprintf("user:%s", userID)
	err := redisClient.Del(redisCtx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to invalidate user cache: %v", err)
	}
	return nil
}
