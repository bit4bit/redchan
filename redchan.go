//RedChan
//go lang chan over redis
package redchan

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"time"
)

const (
	//Where client send and recv data
	FORMAT_CHANNEL_KEY       = "redchan:channel:%s"
	FORMAT_CHANNEL_KEY_CLOSE = "redchan:channel:close:%s"
	FORMAT_CHANNEL_RECV_KEY  = "redchan:channel:recv:%s"
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
	sendCloseKey := fmt.Sprintf(FORMAT_CHANNEL_KEY_CLOSE, channel.ChannelID())
	recvKey := fmt.Sprintf(FORMAT_CHANNEL_RECV_KEY, channel.ChannelID())

	queue := make(chan chan []byte, channel.ChannelSize()+1)
	conn.Do("DEL", recvKey)
	conn.Do("DEL", sendKey)
	conn.Do("DEL", sendCloseKey)

	go func() {
		defer conn.Close()
		for redchan := range queue {
			data, closed := <-redchan
			if !closed {
				break
			}

			_, replyErr := conn.Do("LPUSH", sendKey, data)
			if replyErr != nil {
				panic(replyErr)
			}

			_, replyErr = conn.Do("BRPOP", recvKey, 0)
			if replyErr != nil {
				panic(replyErr)
			}

		}

		_, replyErr := conn.Do("LPUSH", sendCloseKey, true)
		if replyErr != nil {
			panic(replyErr)
		}
	}()

	send := func() chan<- []byte {
		redchan := make(chan []byte)
		queue <- redchan

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
	defer conn.Close()
	sendKey := fmt.Sprintf(FORMAT_CHANNEL_KEY, channel.ChannelID())
	sendCloseKey := fmt.Sprintf(FORMAT_CHANNEL_KEY_CLOSE, channel.ChannelID())
	recvKey := fmt.Sprintf(FORMAT_CHANNEL_RECV_KEY, channel.ChannelID())

	_, replyErr := conn.Do("LPUSH", sendCloseKey, true)
	if replyErr != nil {
		panic(replyErr)
	}
	<-time.After(time.Millisecond * 500)
	conn.Do("DEL", sendKey)
	conn.Do("DEL", sendCloseKey)
	conn.Do("DEL", recvKey)
}

func doRecvRedChan(pool *redis.Pool, redchan chan<- []byte, size int, channelID string) {

	defer pool.Close()

	recvKey := fmt.Sprintf(FORMAT_CHANNEL_KEY, channelID)
	sendKey := fmt.Sprintf(FORMAT_CHANNEL_RECV_KEY, channelID)
	sendCloseKey := fmt.Sprintf(FORMAT_CHANNEL_KEY_CLOSE, channelID)

	recvData := make(chan []byte, size+1)
	stopPoll := make(chan struct{})
	stopChan := make(chan struct{})

	go func() {
		conn := pool.Get()
		defer conn.Close()
		for {
			select {
			case <-stopPoll:
				close(redchan)
				return
			default:
				closedKey, replyCloseErr := redis.Bool(conn.Do("RPOP", sendCloseKey))
				if replyCloseErr != nil && replyCloseErr != redis.ErrNil {
					panic(replyCloseErr)
				}

				if closedKey {
					close(stopChan)
				}

				var data []byte
				<-time.After(time.Millisecond * 100)
				data, replyErr := redis.Bytes(conn.Do("RPOP", recvKey))
				if replyErr != nil {
					switch replyErr {
					case redis.ErrNil:
						continue
					default:
						panic(replyErr)
					}
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
		case <-stopChan:
			//notify we close it
			close(stopPoll)
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
