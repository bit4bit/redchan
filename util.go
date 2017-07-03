package redchan

import (
	"bytes"
	"encoding/gob"
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
		redisAddress = defaultRedisAddress
		redisAuth = defaultRedisAuth
	}
	return redisAddress, redisAuth
}

func encode(data interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode(raw []byte, val interface{}) error {
	buf := bytes.NewBuffer(raw)
	if err := gob.NewDecoder(buf).Decode(val); err != nil {
		return err
	}
	return nil
}
