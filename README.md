redchan
=======

GO Chan over Redis.

We use like go chan.

**install:**

~~~bash
go get github.com/bit4bit/redchan
~~~

example unbuffered:



~~~go
...

redisChannel := RedisChannel{"channeluuid", 0}
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
}()

<-recvCh
<-recvCh

....
~~~

example buffered:

~~~go
...

redisChannel := RedisChannel{"channeluuid", 2}
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


<-recvCh
<-recvCh

....
~~~