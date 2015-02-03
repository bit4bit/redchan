package redchan

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

const TEST_PORT_REDIS = 5445

func TestBuffered(t *testing.T) {
	redisAddress := fmt.Sprintf(":%d", TEST_PORT_REDIS)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(TEST_PORT_REDIS)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{"testbuf", 2}
	sendCh, sendErr := Send(redisChannel)
	if sendErr != nil {
		t.Fatal(sendErr)
	}

	recvCh, recvErr := Recv(redisChannel)
	if recvErr != nil {
		t.Fatal(recvErr)
	}
	defer Close(redisChannel)

	done := make(chan bool)
	go func() {
		sendCh() <- []byte("test1")
		sendCh() <- []byte("test2")
		close(done)
	}()

	select {
	case <-time.After(time.Second * 3):
		t.Fatal("timeout")
	case <-done:
	}

	if string(<-recvCh) != "test1" {
		t.Fatal("invalid order")
	}

	if string(<-recvCh) != "test2" {
		t.Fatal("invalid order")
	}
}

func TestSingleSend(t *testing.T) {
	redisAddress := fmt.Sprintf(":%d", TEST_PORT_REDIS)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(TEST_PORT_REDIS)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{"test", 0}
	sendCh, sendErr := Send(redisChannel)
	if sendErr != nil {
		t.Fatal(sendErr)
	}
	recvCh, recvErr := Recv(redisChannel)
	if recvErr != nil {
		t.Fatal(recvErr)
	}
	defer Close(redisChannel)

	go func() {
		sendCh() <- []byte("end")
	}()

	select {
	case <-time.After(time.Second * 3):
		t.Fatal("timeout")
	case data := <-recvCh:
		if string(data) != "end" {
			t.Fatal("invalid data")
		}
	}

}

func TestBlockingSendChan(t *testing.T) {
	redisAddress := fmt.Sprintf(":%d", TEST_PORT_REDIS)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(TEST_PORT_REDIS)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{"testb", 0}
	sendCh, sendErr := Send(redisChannel)
	if sendErr != nil {
		t.Fatal(sendErr)
	}
	recvCh, recvErr := Recv(redisChannel)
	if recvErr != nil {
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
	case <-time.After(time.Second * 2):
	case <-done:
		t.Fatal("block send channel")
	}

	done2 := make(chan struct{})

	go func() {

		data := string(<-recvCh)
		if "test1" != data {
			t.Fatalf("invalid data was %s", data)
		}

		data = string(<-recvCh)
		if "test2" != data {
			t.Fatal("invalid data was %s", data)
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
	redisAddress := fmt.Sprintf(":%d", TEST_PORT_REDIS)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(TEST_PORT_REDIS)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{"testc", 0}
	sendCh, sendErr := Send(redisChannel)
	if sendErr != nil {
		t.Fatal(sendErr)
	}
	recvCh, recvErr := Recv(redisChannel)
	if recvErr != nil {
		t.Fatal(recvErr)
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
	redisAddress := fmt.Sprintf(":%d", TEST_PORT_REDIS)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(TEST_PORT_REDIS)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{"testa", 0}
	sendCh, sendErr := Send(redisChannel)
	if sendErr != nil {
		t.Fatal(sendErr)
	}
	recvCh, recvErr := Recv(redisChannel)
	if recvErr != nil {
		t.Fatal(recvErr)
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
			if string(data) != expect {
				t.Fatal("invalid order")
			}
			if string(data) == "end" {
				break loop
			}

		}
	}

}

func TestCloseSend(t *testing.T) {
	redisAddress := fmt.Sprintf(":%d", TEST_PORT_REDIS)
	SetRedis(redisAddress)

	redis, redisErr := runRedisServer(TEST_PORT_REDIS)
	if redisErr != nil {
		t.Fatal(redisErr)
	}
	defer redis.Kill()
	time.Sleep(time.Second * 2)

	redisChannel := RedisChannel{"tescloset", 0}
	sendCh, sendErr := Send(redisChannel)
	if sendErr != nil {
		t.Fatal(sendErr)
	}
	recvCh, recvErr := Recv(redisChannel)
	if recvErr != nil {
		t.Fatal(recvErr)
	}
	defer Close(redisChannel)

	go func() {
		sendCh() <- []byte("a")
		sendCh() <- []byte("b")
		close(sendCh())
	}()

	if string(<-recvCh) != "a" {
		t.Fatal("invalid data")
	}

	if string(<-recvCh) != "b" {
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
