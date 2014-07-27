//RedChan
//go lang chan over redis
package redchan

import (
	//	"container/list"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

const (
	//Where client send and recv data
	FORMAT_CHANNEL_KEY      = "redchan:channel:%s"
	FORMAT_CHANNEL_RECV_KEY = "redchan:channel:recv:%s"
)

var (
	DEFAULT_REDIS_ADDRESS = ":6379"
	DEFAULT_REDIS_AUTH    = ""
)

//SetRedis assign default redis addres and auth
func SetRedis(addr string, auth ...string) {
	DEFAULT_REDIS_ADDRESS = addr
	if len(auth) > 0 {
		DEFAULT_REDIS_AUTH = auth[0]
	} else {
		DEFAULT_REDIS_AUTH = ""
	}

}

//Wrap chan
type SendFunc func() chan<- []byte

type RedChan interface {
	ChannelID() string
	ChannelSize() int
}

type RedisChannel struct {
	id   string
	size int
}

func (rd RedisChannel) ChannelID() string {
	return rd.id
}

func (rd RedisChannel) ChannelSize() int {
	return rd.size
}

//Send create chan for send to recv
func Send(channel RedChan, params ...string) (SendFunc, error) {
	pool := newPool(getRedisParams(params...))
	_, connErr := pool.Dial()
	if connErr != nil {
		return nil, connErr
	}
	conn := pool.Get()

	sendKey := fmt.Sprintf(FORMAT_CHANNEL_KEY, channel.ChannelID())
	recvKey := fmt.Sprintf(FORMAT_CHANNEL_RECV_KEY, channel.ChannelID())

	queue := make(chan chan []byte, channel.ChannelSize()+1)

	go func() {
		defer conn.Close()
		for redchan := range queue {
			data := <-redchan

			_, replyErr := conn.Do("LPUSH", sendKey, data)
			if replyErr != nil {
				panic(replyErr)
			}

			_, replyErr = conn.Do("BRPOP", recvKey, 0)
			if replyErr != nil {
				panic(replyErr)
			}

		}
	}()
	send := func() chan<- []byte {
		redchan := make(chan []byte)
		queue <- (redchan)

		return redchan
	}
	return send, nil
}

//Recv create chan for recv from send
func Recv(channel RedChan, params ...string) (<-chan []byte, error) {
	pool := newPool(getRedisParams(params...))
	_, connErr := pool.Dial()
	if connErr != nil {
		return nil, connErr
	}

	//recive chan data <-
	redchan := make(chan []byte, channel.ChannelSize())
	go doRecvRedChan(pool, redchan, channel.ChannelSize(), channel.ChannelID())
	return redchan, nil
}

//Close Channel
func Close(channel RedChan, params ...string) {
	pool := newPool(getRedisParams(params...))
	conn := pool.Get()
	conn.Do("DEL", channel.ChannelID())
	conn.Close()
}

func doRecvRedChan(pool *redis.Pool, redchan chan<- []byte, size int, channelID string) {
	var keyID string

	defer pool.Close()

	recvKey := fmt.Sprintf(FORMAT_CHANNEL_KEY, channelID)
	sendKey := fmt.Sprintf(FORMAT_CHANNEL_RECV_KEY, channelID)
	recvData := make(chan []byte, size+1)
	stop := make(chan struct{})

	go func() {
		conn := pool.Get()
		defer conn.Close()

		for {
			select {
			case <-stop:
				break
			default:
				var data []byte

				reply, replyErr := redis.Values(conn.Do("BRPOP", recvKey, 0))
				if replyErr != nil {
					panic(replyErr)
				}

				if _, err := redis.Scan(reply, &keyID, &data); err != nil {
					panic(err)
				}

				recvData <- data

			}
		}
	}()

	conn := pool.Get()
	defer conn.Close()
loop:
	for {
		select {
		//auto close when no more data
		case <-time.After(time.Second * 30):
			//notify we close it
			close(stop)
			close(redchan)
			break loop
		case data := <-recvData:
			redchan <- data

			_, replyErr := conn.Do("LPUSH", sendKey, "ok")

			if replyErr != nil {
				panic(replyErr)
			}

		}

	}

}
