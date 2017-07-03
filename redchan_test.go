package redchan

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

const testPortRedis = 5445

type msg struct {
	Data []byte
}

func TestBuffered(t *testing.T) {
	redisAddress := fmt.Sprintf(":%d", testPortRedis)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(testPortRedis)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{msg{}, "testbuf", 2}
	sendCh, sendErr := Send(redisChannel)
	if err := sendErr(); err != nil {
		t.Fatal(sendErr)
	}

	recvCh, recvErr := Recv(redisChannel)
	if err := recvErr(); err != nil {
		t.Fatal(recvErr)
	}
	defer Close(redisChannel)

	done := make(chan bool)
	testn := 1000
	wg := sync.WaitGroup{}
	go func() {
		for i := 0; i < testn; i++ {
			wg.Add(1)
			sendCh() <- msg{[]byte(fmt.Sprintf("test%d", i))}
			wg.Done()
		}
		close(done)
	}()
	wg.Wait()

	for i := 0; i < testn; i++ {
		msg := (<-recvCh).(msg)
		expected := fmt.Sprintf("test%d", i)
		was := string(msg.Data)
		if was != expected {
			t.Fatalf("was %s expected %s", was, expected)
		}
	}
}

func TestSingleSend(t *testing.T) {
	redisAddress := fmt.Sprintf(":%d", testPortRedis)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(testPortRedis)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{[]byte{}, "test", 0}

	sendCh, sendErr := Send(redisChannel)
	if err := sendErr(); err != nil {
		t.Fatal(err)
	}
	recvCh, recvErr := Recv(redisChannel)
	if err := recvErr(); err != nil {
		t.Fatal(recvErr)
	}
	defer Close(redisChannel)

	go func() {
		sendCh() <- []byte("end")
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("timeout")
	case data := <-recvCh:
		buf := bytes.NewBuffer(data.([]byte))
		if buf.String() != "end" {
			t.Fatal("invalid data")
		}
	}

}

func TestBlockingSendChan(t *testing.T) {
	redisAddress := fmt.Sprintf(":%d", testPortRedis)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(testPortRedis)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{[]byte{}, "testb", 0}

	sendCh, sendErr := Send(redisChannel)
	if err := sendErr(); err != nil {
		t.Fatal(sendErr)
	}
	recvCh, recvErr := Recv(redisChannel)
	if err := recvErr(); err != nil {
		t.Fatal(recvErr)
	}
	defer Close(redisChannel)
	done := make(chan struct{})
	go func() {
		sendCh() <- []byte("test1")
		sendCh() <- []byte("test2")
		close(done)
	}()
	select {
	case <-time.After(time.Second):
	case <-done:
		t.Fatal("block send channel")
	}

	done2 := make(chan struct{})

	go func() {

		data := string((<-recvCh).([]byte))
		if "test1" != data {
			t.Fatalf("invalid data was %s", data)
		}

		data = string((<-recvCh).([]byte))
		if "test2" != data {
			t.Fatalf("invalid data was %s", data)
		}
		sendCh() <- []byte("test3")

		done2 <- struct{}{}
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("timeout")
	case <-done2:
	}
}

func TestBlockingRecvChan(t *testing.T) {
	redisAddress := fmt.Sprintf(":%d", testPortRedis)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(testPortRedis)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{[]byte{}, "testc", 0}

	sendCh, sendErr := Send(redisChannel)
	if err := sendErr(); err != nil {
		t.Fatal(err)
	}
	recvCh, recvErr := Recv(redisChannel)
	if err := recvErr(); err != nil {
		t.Fatal(err)
	}
	defer Close(redisChannel)

	sendCh() <- []byte("test")
	<-recvCh
	select {
	case <-time.After(time.Second):
	case <-recvCh:
		t.Fatal("block send channel")
	}

	go func() {
		sendCh() <- []byte("test2")
	}()

	select {
	case <-time.After(time.Second):
		t.Fatal("timeout")
	case <-recvCh:
	}
}

func TestSequenceSend(t *testing.T) {
	redisAddress := fmt.Sprintf(":%d", testPortRedis)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(testPortRedis)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{[]byte{}, "testa", 0}

	sendCh, sendErr := Send(redisChannel)
	if err := sendErr(); err != nil {
		t.Fatal(err)
	}
	recvCh, recvErr := Recv(redisChannel)
	if err := recvErr(); err != nil {
		t.Fatal(err)
	}
	defer Close(redisChannel)
	expectCh := make(chan string)
	fixture := []string{"a", "b", "c", "d", "e", "end"}

	go func() {

		for _, expect := range fixture {
			sendCh() <- []byte(expect)
			expectCh <- expect
		}

	}()

loop:
	for {

		select {
		case <-time.After(time.Second):
			t.Fatal("timeout")
		case data := <-recvCh:
			expect := <-expectCh
			if string(data.([]byte)) != expect {
				t.Fatal("invalid order")
			}
			if string(data.([]byte)) == "end" {
				break loop
			}

		}
	}

}

func TestCloseSend(t *testing.T) {
	redisAddress := fmt.Sprintf(":%d", testPortRedis)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(testPortRedis)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{[]byte{}, "tescloset", 0}
	sendCh, sendErr := Send(redisChannel)
	if err := sendErr(); err != nil {
		t.Fatal(err)
	}
	recvCh, recvErr := Recv(redisChannel)
	if err := recvErr(); err != nil {
		t.Fatal(err)
	}
	defer Close(redisChannel)

	go func() {
		sendCh() <- []byte("a")
		sendCh() <- []byte("b")
		close(sendCh())
	}()

	if string((<-recvCh).([]byte)) != "a" {
		t.Fatal("invalid data")
	}

	if string((<-recvCh).([]byte)) != "b" {
		t.Fatal("invalid data")
	}
	_, closed := <-recvCh
	if closed != false {
		t.Fatal("closing")
	}
}

func runRedisServer(port int) (*os.Process, error) {
	cmd := exec.Command("redis-server", "--port", fmt.Sprintf("%d", port))
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd.Process, nil
}
