//Package redchan go lang chan over redis
package redchan

import (
	"context"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"time"
)

const (
	formatChannelRedisKey      = "redchan:channel:%s"
	formatChannelRedisKeyClose = "redchan:channel:close:%s"
	formatChannelRedisRecvKey  = "redchan:channel:recv:%s"
)

var (
	defaultRedisAddress = ":6379"
	defaultRedisAuth    = ""
)

//SetRedis assign default redis addres and auth
func SetRedis(addr string, auth ...string) {
	defaultRedisAddress = addr
	if len(auth) > 0 {
		defaultRedisAuth = auth[0]
	} else {
		defaultRedisAuth = ""
	}

}

//SendFunc Wrap chan
type SendFunc func() chan<- interface{}

//ErrorFunc return last error
//If this return a error means you need call again Send/Recv
//because all connections are closed.
type ErrorFunc func() error

type redChan interface {
	ChannelID() string
	ChannelSize() int
	ChannelKind() reflect.Type
}

//RedisChannel identify a unique channel on redis
//for send and receive messages
type RedisChannel struct {
	kind interface{}
	id   string
	size int
}

//ChannelID unique id for channel
func (rd RedisChannel) ChannelID() string {
	return rd.id
}

//ChannelSize size of channel
func (rd RedisChannel) ChannelSize() int {
	return rd.size
}

//ChannelKind kind of channel for decoding and encoding any struct
func (rd RedisChannel) ChannelKind() reflect.Type {
	typ := reflect.TypeOf(rd.kind)
	switch typ.Kind() {
	case reflect.Ptr:
		return typ.Elem()
	default:
		return typ
	}
}

//Send create chan for send to recv
func Send(channel redChan, params ...string) (SendFunc, ErrorFunc) {
	errCh := make(chan error, 1)
	fail := func() error {
		select {
		case err := <-errCh:
			return err
		default:
			return nil
		}
	}
	pool := newPool(getRedisParams(params...))
	_, connErr := pool.Dial()
	if connErr != nil {
		return nil, fail
	}
	conn := pool.Get()

	sendKey := fmt.Sprintf(formatChannelRedisKey, channel.ChannelID())
	sendCloseKey := fmt.Sprintf(formatChannelRedisKeyClose, channel.ChannelID())
	recvKey := fmt.Sprintf(formatChannelRedisRecvKey, channel.ChannelID())

	queue := make(chan chan interface{}, channel.ChannelSize()+1)

	go func() {
		defer conn.Close()
		for redchan := range queue {
			data, closed := <-redchan
			if !closed {
				break
			}
			raw, errEncode := encode(data)

			if errEncode != nil {
				errCh <- errEncode
				break
			}

			_, replyErr := conn.Do("LPUSH", sendKey, raw)
			if replyErr != nil {
				errCh <- replyErr
				break
			}

			_, replyErr = conn.Do("BRPOP", recvKey, 0)
			if replyErr != nil {
				errCh <- replyErr
				break
			}

		}

		_, replyErr := conn.Do("LPUSH", sendCloseKey, true)
		if replyErr != nil {
			errCh <- replyErr
		}
	}()

	send := func() chan<- interface{} {
		redchan := make(chan interface{})
		queue <- redchan
		return redchan
	}

	return send, fail
}

//Recv create chan for recv from send
func Recv(channel redChan, params ...string) (<-chan interface{}, ErrorFunc) {
	errch := make(chan error, 1)
	fail := func() error {
		select {
		case err := <-errch:
			return err
		default:
			return nil
		}
	}

	pool := newPool(getRedisParams(params...))
	_, connErr := pool.Dial()
	if connErr != nil {
		return nil, fail
	}

	//recive chan data <-
	redchan := make(chan interface{}, channel.ChannelSize())
	go doRecvRedChan(pool, channel, redchan, errch, channel.ChannelSize(), channel.ChannelID())
	return redchan, fail
}

//Close Channel
func Close(channel redChan, params ...string) {
	pool := newPool(getRedisParams(params...))
	conn := pool.Get()
	defer conn.Close()
	sendKey := fmt.Sprintf(formatChannelRedisKey, channel.ChannelID())
	sendCloseKey := fmt.Sprintf(formatChannelRedisKeyClose, channel.ChannelID())
	recvKey := fmt.Sprintf(formatChannelRedisRecvKey, channel.ChannelID())

	_, replyErr := conn.Do("LPUSH", sendCloseKey, true)
	if replyErr != nil {
		panic(replyErr)
	}
	<-time.After(time.Millisecond * 500)
	conn.Do("DEL", sendKey)
	conn.Do("DEL", sendCloseKey)
	conn.Do("DEL", recvKey)
}

func doRecvRedChan(
	pool *redis.Pool,
	channel redChan,
	redchan chan<- interface{},
	errch chan<- error,
	size int, channelID string,
) {

	defer pool.Close()

	recvKey := fmt.Sprintf(formatChannelRedisKey, channelID)
	sendKey := fmt.Sprintf(formatChannelRedisRecvKey, channelID)
	sendCloseKey := fmt.Sprintf(formatChannelRedisKeyClose, channelID)

	recvData := make(chan interface{}, size+1)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		conn := pool.Get()
		defer conn.Close()
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
				closedKey, replyCloseErr := redis.Bool(conn.Do("RPOP", sendCloseKey))
				if replyCloseErr != nil && replyCloseErr != redis.ErrNil {
					errch <- replyCloseErr
					cancel()
					break loop
				}

				if closedKey {
					cancel()
					break loop
				}
			}
		}
	}()
	go func() {
		conn := pool.Get()
		defer conn.Close()

	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
				var data []byte
				data, replyErr := redis.Bytes(conn.Do("RPOP", recvKey))
				if replyErr != nil {
					switch replyErr {
					case redis.ErrNil:
						continue
					default:
						errch <- replyErr
						cancel()
						break loop
					}
				}

				val := reflect.New(channel.ChannelKind()).Interface()

				if err := decode(data, val); err != nil {
					errch <- err
				} else {
					switch channel.ChannelKind().Kind() {
					case reflect.Ptr:
						recvData <- val
					default:
						typ := reflect.TypeOf(val)

						switch typ.Kind() {
						case reflect.Ptr:
							vl := reflect.ValueOf(val)
							recvData <- vl.Elem().Interface()
						default:
							recvData <- val
						}
					}
				}

			}
		}
	}()

	conn := pool.Get()
	defer conn.Close()
loop:
	for {
		select {
		//auto close when no more data
		case <-ctx.Done():
			close(redchan)
			break loop
		case data := <-recvData:
			redchan <- data
			_, replyErr := conn.Do("LPUSH", sendKey, "ok")

			if replyErr != nil {
				errch <- replyErr
				cancel()
			}
		}

	}

}
