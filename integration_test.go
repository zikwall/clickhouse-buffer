// +build integration

package clickhousebuffer

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/ory/dockertest/v3"
	"log"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	var db *redis.Client

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.Run("redis", "6.2", nil)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	if err := pool.Retry(func() error {
		db = redis.NewClient(&redis.Options{
			Addr: fmt.Sprintf("localhost:%s", resource.GetPort("6379/tcp")),
		})

		return db.Ping(db.Context()).Err()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	if err := db.Set(context.Background(), "test_key", "test_value", 10*time.Second).Err(); err != nil {
		log.Fatalf("Could not set value: %s", err)
	}

	if value := db.Get(context.Background(), "test_key").Val(); value != "test_value" {
		log.Fatalf("Could not get correct value, received: %s", value)
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestSomething(t *testing.T) {
	// db.Query()
}
