package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

import "crypto/rand"
import "math/big"

import (
	"log"
	"time"
)

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	// By Yan
	curView          viewservice.View
	ackedPutAppendId int64
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	// By Yan
	ck.curView.Primary = ""
	ck.curView.Backup = ""
	ck.curView.Viewnum = 0

	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

	// Your code here.
	// By Yan
	var args GetArgs
	var reply GetReply

	log.Printf("Get curView.Primary is %s", ck.curView.Primary)

	if ck.curView.Primary == "" {
		//log.Printf("Get no Primary server 1\n")
		vx, _ := ck.vs.Get()
		ck.curView = vx
	}

	args.Key = key
	args.Id = nrand()
	log.Printf("id is %d", args.Id)
	c := make(chan GetRes, 1)
	lastPrimary := ""
	for {
		if ck.curView.Primary != lastPrimary {
			log.Printf("Get has Primary server %s\n", ck.curView.Primary)
			lastPrimary = ck.curView.Primary
			server := ck.curView.Primary
			argsR := args
			replyR := reply
			go func() {
				var res GetRes
				res.Succeed = call(server, "PBServer.Get", &argsR, &replyR)
				res.Reply = replyR
				c <- res
			}()
		} else {
			//log.Printf("Get no Primary server 2\n")
		}
		select {
		case res := <-c:
			if res.Succeed == false {
				log.Printf("get failed")
				vx, _ := ck.vs.Get()
				ck.curView = vx
			} else {
				log.Printf("reply.Me is %s", res.Reply.Me)
				log.Printf("reply.Value is %s", res.Reply.Value)
				if res.Reply.Me != ck.curView.Primary {
					continue
				}
				return res.Reply.Value
			}
			//case <-time.After(viewservice.PingInterval * viewservice.DeadPings):
			//	log.Printf("Get expired\n")
			//	log.Printf("cur primary is %s", ck.curView.Primary)
			//	vx, _ := ck.vs.Get()
			//	ck.curView = vx
			//	log.Printf("cur primary is %s", ck.curView.Primary)
		}
	}
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	// By Yan
	var args PutAppendArgs
	var reply PutAppendReply

	log.Printf("PutAppend curView.Primary is %s", ck.curView.Primary)

	if ck.curView.Primary == "" {
		vx, _ := ck.vs.Get()
		ck.curView = vx
	}

	log.Printf("PutAppend curView.Primary is %s", ck.curView.Primary)

	args.Key = key
	args.Value = value
	args.Op = op
	args.Id = nrand()
	args.AckedId = ck.ackedPutAppendId
	c := make(chan bool, 1)
	for {
		if ck.curView.Primary != "" {
			go func() {
				c <- call(ck.curView.Primary, "PBServer.PutAppend", &args, &reply)
			}()
		} else {
			log.Printf("PutAppend no Primary server\n")
		}
		select {
		case succeed := <-c:
			if succeed == false {
				vx, _ := ck.vs.Get()
				ck.curView = vx
			} else {
				ck.ackedPutAppendId = args.Id
				return
			}
		case <-time.After(viewservice.PingInterval * viewservice.DeadPings):
			log.Printf("PutAppend expired\n")
			vx, _ := ck.vs.Get()
			ck.curView = vx
		}
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
