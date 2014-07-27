package redchan

import (
	"github.com/garyburd/redigo/redis"
	"strconv"
	"time"
)

func newPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxActive:   0,
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

//GENERATE uuid using *key*
func getUUID(pool *redis.Pool, key string) (string, error) {
	conn := pool.Get()
	defer conn.Close()
	uuid, err := redis.Int64(conn.Do("INCR", key))
	if err != nil {
		return "", nil
	}

	return strconv.FormatInt(uuid, 10), nil
}

func getRedisParams(params ...string) (string, string) {
	var redisAuth string
	var redisAddress string
	switch len(params) {
	case 2:
		redisAddress = params[0]
		redisAuth = params[1]
	case 1:
		redisAddress = params[0]
	case 0:
		redisAddress = DEFAULT_REDIS_ADDRESS
		redisAuth = DEFAULT_REDIS_AUTH
	}
	return redisAddress, redisAuth
}
