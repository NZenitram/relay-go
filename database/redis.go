package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"relay-go/m/logger"
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
			logger.Warning(redisCtx, "redis", "Redis authentication failed, retrying without password", err)
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

	logger.Info(redisCtx, "redis", "Successfully connected to Redis")
	return nil
}

// GetRedisClient returns the Redis client
func GetRedisClient() *redis.Client {
	if redisClient == nil {
		logger.Fatal(redisCtx, "redis", "Redis client has not been initialized. Call InitRedis() first.", nil)
	}
	return redisClient
}

// CloseRedis closes the Redis connection
func CloseRedis() {
	if redisClient != nil {
		redisClient.Close()
		logger.Info(redisCtx, "redis", "Redis connection closed")
	}
}

// CacheUserData caches user data in Redis
func CacheUserData(userID string, userData interface{}) error {
	ctx := logger.WithUserID(redisCtx, userID)
	key := fmt.Sprintf("user:%s", userID)

	data, err := json.Marshal(userData)
	if err != nil {
		logger.Error(ctx, "redis", "Failed to marshal user data", err)
		return fmt.Errorf("failed to marshal user data: %v", err)
	}

	// Cache for 1 hour
	err = redisClient.Set(ctx, key, data, time.Hour).Err()
	if err != nil {
		logger.Error(ctx, "redis", "Failed to cache user data", err)
		return fmt.Errorf("failed to cache user data: %v", err)
	}

	logger.Info(ctx, "redis", "Successfully cached user data")
	return nil
}

// GetCachedUserData retrieves user data from Redis cache
func GetCachedUserData(userID string, userData interface{}) (bool, error) {
	ctx := logger.WithUserID(redisCtx, userID)
	key := fmt.Sprintf("user:%s", userID)

	data, err := redisClient.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			logger.Info(ctx, "redis", "Cache miss for user data")
			return false, nil
		}
		logger.Error(ctx, "redis", "Failed to get cached user data", err)
		return false, fmt.Errorf("failed to get cached user data: %v", err)
	}

	err = json.Unmarshal([]byte(data), userData)
	if err != nil {
		logger.Error(ctx, "redis", "Failed to unmarshal cached user data", err)
		return false, fmt.Errorf("failed to unmarshal cached user data: %v", err)
	}

	logger.Info(ctx, "redis", "Cache hit for user data")
	return true, nil
}

// InvalidateUserCache removes user data from Redis cache
func InvalidateUserCache(userID string) error {
	ctx := logger.WithUserID(redisCtx, userID)
	key := fmt.Sprintf("user:%s", userID)

	err := redisClient.Del(ctx, key).Err()
	if err != nil {
		logger.Error(ctx, "redis", "Failed to invalidate user cache", err)
		return fmt.Errorf("failed to invalidate user cache: %v", err)
	}

	logger.Info(ctx, "redis", "Successfully invalidated user cache")
	return nil
}
