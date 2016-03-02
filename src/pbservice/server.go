package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

import "errors"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	// By Yan
	curView                viewservice.View
	db                     map[string]string
	lastGetId              int64
	lastPutAppendId        int64
	lastForwardGetId       int64
	lastForwardPutAppendId int64
	lastReplicateAllId     int64
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	// By Yan
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//log.Printf("Get %s\n", pb.me)
	if pb.me != pb.curView.Primary {
		return errors.New(ErrWrongServer)
	}

	if pb.lastGetId != args.Id {
		pb.lastGetId = args.Id
	}

	v, exist := pb.db[args.Key]
	if !exist {
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Err = OK
		reply.Value = v
	}

	if pb.curView.Backup != "" {
		var replyU GetReply
		timedout := true

		log.Printf("Server Get has Backup server 1\n")

		c := make(chan bool, 1)
		for timedout {
			go func() {
				c <- call(pb.curView.Backup, "PBServer.ForwardGet", args, &replyU)
			}()
			select {
			case succeed := <-c:
				if !succeed {
					//its failed because client is primary
					vx, _ := pb.vs.Get()
					pb.curView = vx
					return errors.New(ErrWrongServer)
				} else {
					timedout = false
				}
			case <-time.After(viewservice.PingInterval * viewservice.DeadPings):
				log.Printf("Server Get expired\n")
				vx, _ := pb.vs.Get()
				pb.curView = vx
				if pb.curView.Backup == "" {
					timedout = false
				}
			}
		}
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	// By Yan
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//log.Printf("PutAppend\n")
	if pb.me != pb.curView.Primary {
		return errors.New(ErrWrongServer)
	}

	if pb.lastPutAppendId == args.Id {
		reply.Err = OK
		return nil
	} else {
		pb.lastPutAppendId = args.Id
	}

	if args.Op == "Put" {
		pb.db[args.Key] = args.Value
	} else {
		v, exist := pb.db[args.Key]
		if !exist {
			pb.db[args.Key] = args.Value
		} else {
			pb.db[args.Key] = v + args.Value
		}
	}

	log.Printf("Server PutAppend curView.Backup is %s", pb.curView.Backup)

	if pb.curView.Backup != "" {
		var replyU PutAppendReply
		timedout := false

		log.Printf("Server PutAppend has Backup server 1\n")

		c := make(chan bool, 1)
		for timedout {
			go func() {
				c <- call(pb.curView.Backup, "PBServer.ForwardPutAppend", args, &replyU)
			}()
			select {
			case succeed := <-c:
				if !succeed {
					//its failed because client is primary
					vx, _ := pb.vs.Get()
					pb.curView = vx
					return errors.New(ErrWrongServer)
				} else {
					timedout = false
				}
			case <-time.After(viewservice.PingInterval * viewservice.DeadPings):
				log.Printf("Server PutAppend expired\n")
				vx, _ := pb.vs.Get()
				pb.curView = vx
				if pb.curView.Backup == "" {
					timedout = false
				}
			}
		}
	}

	reply.Err = OK

	return nil
}

// By Yan
func (pb *PBServer) ForwardGet(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//log.Printf("Forward\n")
	if pb.me != pb.curView.Backup || pb.me == pb.curView.Primary {
		return errors.New(ErrWrongServer)
	}

	if pb.lastForwardGetId != args.Id {
		pb.lastForwardGetId = args.Id
	}

	reply.Err = OK

	return nil
}

// By Yan
func (pb *PBServer) ForwardPutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//log.Printf("Forward\n")
	if pb.me != pb.curView.Backup || pb.me == pb.curView.Primary {
		return errors.New(ErrWrongServer)
	}

	if pb.lastForwardPutAppendId == args.Id {
		reply.Err = OK
		return nil
	} else {
		pb.lastForwardPutAppendId = args.Id
	}

	//log.Printf("Forward 2\n")
	if args.Op == "Put" {
		pb.db[args.Key] = args.Value
	} else {
		v, exist := pb.db[args.Key]
		if !exist {
			pb.db[args.Key] = args.Value
		} else {
			pb.db[args.Key] = v + args.Value
		}
	}

	reply.Err = OK

	return nil
}

// By Yan
func (pb *PBServer) ReplicateAll(args *ReplicateAllArgs, reply *ReplicateAllReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//log.Printf("ReplicateAll, %s\n", pb.me)
	if pb.me != pb.curView.Backup {
		return errors.New(ErrWrongServer)
	}

	if pb.lastReplicateAllId == args.Id {
		reply.Err = OK
		return nil
	} else {
		pb.lastReplicateAllId = args.Id
	}

	for k, v := range args.DB {
		pb.db[k] = v
	}

	reply.Err = OK

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {

	// Your code here.
	// By Yan
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//log.Printf("%s tick", pb.curView.Primary)

	vx, _ := pb.vs.Ping(pb.curView.Viewnum)

	//log.Printf("vx Primary is %s", vx.Primary)
	//log.Printf("vx Backup is %s", vx.Backup)
	//log.Printf("vx Viewnum is %d", vx.Viewnum)

	if pb.curView.Primary == pb.me && pb.curView.Primary != vx.Primary {

	}

	if pb.me == pb.curView.Primary && pb.curView.Backup != vx.Backup && vx.Backup != "" {
		pb.curView = vx

		var args ReplicateAllArgs
		var reply ReplicateAllReply

		args.Me = pb.curView.Primary
		args.DB = make(map[string]string)
		args.Id = nrand()

		for k, v := range pb.db {
			args.DB[k] = v
		}

		//log.Printf("call ReplicateAll")

		log.Printf("Server ReplicateAll curView.Backup is %s", pb.curView.Backup)

		c := make(chan bool, 1)
		for {
			go func() {
				c <- call(pb.curView.Backup, "PBServer.ReplicateAll", &args, &reply)
			}()
			select {
			case success := <-c:
				if success == false {
					vx, _ := pb.vs.Get()
					pb.curView = vx
					if pb.curView.Backup == "" {
						return
					}
				} else {
					return
				}
			case <-time.After(viewservice.PingInterval * viewservice.DeadPings):
				log.Printf("ReplicateAll expired\n")
				vx, _ := pb.vs.Get()
				pb.curView = vx
				if pb.curView.Backup == "" {
					return
				}
			}
		}
	}

	pb.curView = vx
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	log.Printf("startServer %s", me)
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	// By Yan
	pb.db = make(map[string]string)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
