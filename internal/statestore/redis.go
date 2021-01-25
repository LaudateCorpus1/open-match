// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statestore

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/go-redis/redis/v8"
	rs "github.com/go-redsync/redsync/v4"
	rsgoredis "github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"open-match.dev/open-match/internal/config"
)

var (
	redisLogger = logrus.WithFields(logrus.Fields{
		"app":       "openmatch",
		"component": "statestore.redis",
	})

	// this field is used to create new mutexes
	redsync *rs.Redsync
)

// NewMutex returns a new distributed mutex with given name
func (rb *redisBackend) NewMutex(key string) RedisLocker {
	m := redsync.NewMutex(fmt.Sprintf("lock/%s", key), rs.WithExpiry(rb.cfg.GetDuration("backfillLockTimeout")))
	return redisBackend{mutex: m}
}

//Lock locks r. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (rb redisBackend) Lock(ctx context.Context) error {
	return rb.mutex.LockContext(ctx)
}

// Unlock unlocks r and returns the status of unlock.
func (rb redisBackend) Unlock(ctx context.Context) (bool, error) {
	return rb.mutex.UnlockContext(ctx)
}

type redisBackend struct {
	healthCheckPool *redis.Client
	redisPool       *redis.Client
	cfg             config.View
	mutex           *rs.Mutex
}

// Close the connection to the database.
func (rb *redisBackend) Close() error {
	return rb.redisPool.Close()
}

// newRedis creates a statestore.Service backed by Redis database.
func newRedis(cfg config.View) Service {
	pool := GetRedisPool(cfg)
	redsync = rs.New(rsgoredis.NewPool(pool))
	return &redisBackend{
		healthCheckPool: getHealthCheckClient(cfg),
		redisPool:       pool,
		cfg:             cfg,
	}
}

func getHealthCheckClient(cfg config.View) *redis.Client {
	var client *redis.Client
	var healthCheckTimeout = cfg.GetDuration("redis.pool.healthCheckTimeout")

	if cfg.IsSet("redis.sentinelHostname") {
		options := &redis.FailoverOptions{
			MasterName:    cfg.GetString("redis.sentinelMaster"),
			SentinelAddrs: []string{getSentinelAddr(cfg)},
			DialTimeout:   healthCheckTimeout,
			ReadTimeout:   healthCheckTimeout,
			IdleTimeout:   10 * healthCheckTimeout,
		}

		if cfg.GetBool("redis.sentinelUsePassword") {
			passwordFile := cfg.GetString("redis.passwordPath")
			redisLogger.Debugf("loading Redis password from file %s", passwordFile)
			passwordData, err := ioutil.ReadFile(passwordFile)
			if err != nil {
				redisLogger.Fatalf("cannot read Redis password from file %s, desc: %s", passwordFile, err.Error())
			}

			options.Username = cfg.GetString("redis.user")
			options.Password = string(passwordData)
			options.SentinelPassword = string(passwordData)
		}
		client = redis.NewFailoverClient(options)
	} else {
		options := &redis.Options{
			Addr:        getMasterAddr(cfg),
			DialTimeout: healthCheckTimeout,
			ReadTimeout: healthCheckTimeout,
			IdleTimeout: 10 * healthCheckTimeout,
		}

		if cfg.GetBool("redis.sentinelUsePassword") {
			passwordFile := cfg.GetString("redis.passwordPath")
			redisLogger.Debugf("loading Redis password from file %s", passwordFile)
			passwordData, err := ioutil.ReadFile(passwordFile)
			if err != nil {
				redisLogger.Fatalf("cannot read Redis password from file %s, desc: %s", passwordFile, err.Error())
			}

			options.Username = cfg.GetString("redis.user")
			options.Password = string(passwordData)
		}
		client = redis.NewClient(options)
	}

	return client
}

// GetRedisPool configures a new pool to connect to redis given the config.
func GetRedisPool(cfg config.View) *redis.Client {
	var client *redis.Client

	maxActive := cfg.GetInt("redis.pool.maxActive")
	idleTimeout := cfg.GetDuration("redis.pool.idleTimeout")
	idleCheckFrequency := idleTimeout
	if idleTimeout.Milliseconds() > 0 {
		idleCheckFrequency = idleTimeout / 2
	}
	if cfg.IsSet("redis.sentinelHostname") {
		options := &redis.FailoverOptions{
			MasterName:         cfg.GetString("redis.sentinelMaster"),
			SentinelAddrs:      []string{getSentinelAddr(cfg)},
			PoolSize:           maxActive,
			IdleCheckFrequency: idleCheckFrequency,
			IdleTimeout:        idleTimeout,
			MaxConnAge:         idleTimeout,
		}

		if cfg.GetBool("redis.sentinelUsePassword") {
			passwordFile := cfg.GetString("redis.passwordPath")
			redisLogger.Debugf("loading Redis password from file %s", passwordFile)
			passwordData, err := ioutil.ReadFile(passwordFile)
			if err != nil {
				redisLogger.Fatalf("cannot read Redis password from file %s, desc: %s", passwordFile, err.Error())
			}

			options.Username = cfg.GetString("redis.user")
			options.Password = string(passwordData)
			options.SentinelPassword = string(passwordData)
		}
		client = redis.NewFailoverClient(options)
	} else {
		options := &redis.Options{
			Addr:               getMasterAddr(cfg),
			PoolSize:           maxActive,
			IdleCheckFrequency: idleCheckFrequency,
			IdleTimeout:        idleTimeout,
			MaxConnAge:         idleTimeout,
		}

		if cfg.GetBool("redis.usePassword") {
			passwordFile := cfg.GetString("redis.passwordPath")
			redisLogger.Debugf("loading Redis password from file %s", passwordFile)
			passwordData, err := ioutil.ReadFile(passwordFile)
			if err != nil {
				redisLogger.Fatalf("cannot read Redis password from file %s, desc: %s", passwordFile, err.Error())
			}

			options.Username = cfg.GetString("redis.user")
			options.Password = string(passwordData)
		}
		client = redis.NewClient(options)
	}

	return client
}

// HealthCheck indicates if the database is reachable.
func (rb *redisBackend) HealthCheck(ctx context.Context) error {
	cmd := rb.healthCheckPool.Ping(ctx)
	_, err := cmd.Result()
	if err != nil {
		return status.Errorf(codes.Unavailable, "%v", err)
	}
	return nil
}

func (rb *redisBackend) GetConnection(ctx context.Context) (*redis.Client, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return rb.redisPool, nil
	}
}

func getSentinelAddr(cfg config.View) string {
	return fmt.Sprintf("%s:%s", cfg.GetString("redis.sentinelHostname"), cfg.GetString("redis.sentinelPort"))
}

func getMasterAddr(cfg config.View) string {
	return fmt.Sprintf("%s:%s", cfg.GetString("redis.hostname"), cfg.GetString("redis.port"))
}
