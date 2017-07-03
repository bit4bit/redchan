redchan
=======

GO Chan over Redis.

We use like Go chan.

**install:**

~~~bash
go get github.com/bit4bit/redchan
~~~

```example unbuffered:``



~~~go
...

redisChannel := RedisChannel{[]byte{}, "channeluuid", 0}
defer redchan.Close(redisChannel)
sendCh, sendErr := redchan.Send(redisChannel)
if sendErr != nil {
	t.Fatal(sendErr)
}

recvCh, recvErr := redchan.Recv(redisChannel)
if recvErr != nil {
		t.Fatal(recvErr)
}

go func(){
	sendCh() <- []byte("mydata")
	sendCh() <- []byte("mydata..")
	close(sendCh())
}()

for recv := <-recvCh {
	data := recv.([]byte)
	...
}


....
~~~


```example buffered:```

~~~go
...

redisChannel := RedisChannel{[]byte{}, "channeluuid", 2}
defer redchan.Close(redisChannel)
sendCh, sendErr := redchan.Send(redisChannel)
if sendErr != nil {
	t.Fatal(sendErr)
}

recvCh, recvErr := redchan.Recv(redisChannel)
if recvErr != nil {
		t.Fatal(recvErr)
}

sendCh() <- []byte("mydata")
sendCh() <- []byte("mydata..")
close(sendCh())

<-recvCh
<-recvCh

....
~~~


```example encoding decoding:``



~~~go
...

type msg struct {
	Message []byte
	Kind int
}

redisChannel := RedisChannel{msg{}, "channeluuid", 0}
defer redchan.Close(redisChannel)
sendCh, sendErr := redchan.Send(redisChannel)
if sendErr != nil {
	t.Fatal(sendErr)
}

recvCh, recvErr := redchan.Recv(redisChannel)
if recvErr != nil {
		t.Fatal(recvErr)
}

go func(){
	sendCh() <- msg{"hola", 1}
	sendCh() <- msg{"hello", 2}
	close(sendCh())
}()

for recv := <-recvCh {
	mymsg := recv.(msg{})
	...
}


....
~~~
