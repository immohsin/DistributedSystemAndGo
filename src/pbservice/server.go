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
	curView viewservice.View
	db      map[string]string
	//lastGetId              map[int64]time.Time
	lastPutAppendId     map[int64]time.Time
	lastReplicateAllId  map[int64]time.Time
	ackedReplicateAllId int64
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	// By Yan
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//log.Printf("Server Get %s args.key %s\n", pb.me, args.Key)
	//log.Printf("Server Get %s %s\n", pb.curView.Primary, pb.curView.Backup)
	//log.Printf("id is %d", args.Id)
	if pb.me != pb.curView.Primary {
		return errors.New(ErrWrongServer)
	}

	//if pb.lastGetId != args.Id {
	//	pb.lastGetId = args.Id
	//}

	v, exist := pb.db[args.Key]
	if !exist {
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Err = OK
		reply.Value = v
	}

	var replyU GetReply
	c := make(chan bool, 1)
	for {
		if pb.curView.Backup == "" {
			break
		}
		go func() {
			c <- call(pb.curView.Backup, "PBServer.ForwardGet", args, &replyU)
		}()

		succeed := <-c
		if !succeed {
			time.Sleep(viewservice.PingInterval)
			//its failed could because client is primary
			vx, _ := pb.vs.Get()
			pb.curView = vx
			if pb.curView.Primary != pb.me {
				return errors.New(ErrWrongServer)
			}
			if pb.curView.Backup == "" {
				break
			}
		} else {
			break
		}
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	// By Yan
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//fmt.Printf("PutAppend\n")
	if pb.me != pb.curView.Primary {
		return errors.New(ErrWrongServer)
	}

	_, seen := pb.lastPutAppendId[args.Id]
	if seen {
		reply.Err = OK
		return nil
	}

	if args.Op == "Put" {
		pb.db[args.Key] = args.Value
	} else {
		v, exist := pb.db[args.Key]
		if !exist {
			pb.db[args.Key] = args.Value
		} else {
			//log.Printf("append k is %s v is %s", args.Key, args.Value)
			pb.db[args.Key] = v + args.Value
		}
	}

	pb.lastPutAppendId[args.Id] = time.Now()
	//fmt.Printf("Server PutAppend curView.Backup is %s\n", pb.curView.Backup)

	var replyU PutAppendReply
	c := make(chan bool, 1)
	for {
		if pb.curView.Backup == "" {
			break
		}
		//fmt.Printf("Server PutAppend 1\n")
		go func() {
			c <- call(pb.curView.Backup, "PBServer.ForwardPutAppend", args, &replyU)
		}()
		//fmt.Printf("Server PutAppend 2\n")
		succeed := <-c
		//fmt.Printf("Server PutAppend 3\n")
		if !succeed {
			time.Sleep(viewservice.PingInterval)
			//its failed could because client is primary
			vx, _ := pb.vs.Get()
			pb.curView = vx
			if pb.curView.Primary != pb.me {
				//fmt.Printf("Server PutAppend error return\n")
				return errors.New(ErrWrongServer)
			}
			if pb.curView.Backup == "" {
				//log.Printf("Server PutAppend no backup\n")
				break
			}
		} else {
			break
		}
	}
	//fmt.Printf("Server PutAppend finished\n")

	//delete(pb.lastPutAppendId, args.AckedId)
	reply.Err = OK

	return nil
}

// By Yan
func (pb *PBServer) ForwardGet(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//log.Printf("Forward Get\n")
	//log.Printf("Server Forward Get %s %d args.key %s\n", pb.me, pb.viewConnected, args.Key)
	if pb.me != pb.curView.Backup || pb.me == pb.curView.Primary {
		return errors.New(ErrWrongServer)
	}

	//if pb.lastForwardGetId != args.Id {
	//	pb.lastForwardGetId = args.Id
	//}

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

	_, seen := pb.lastPutAppendId[args.Id]
	if seen {
		reply.Err = OK
		return nil
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

	//delete(pb.lastPutAppendId, args.AckedId)
	pb.lastPutAppendId[args.Id] = time.Now()
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

	_, seen := pb.lastReplicateAllId[args.Id]
	if seen {
		reply.Err = OK
		return nil
	}

	for k, v := range args.DB {
		pb.db[k] = v
	}

	for k, v := range args.LastPutAppendId {
		pb.lastPutAppendId[k] = v
	}

	//delete(pb.lastReplicateAllId, args.AckedId)
	pb.lastReplicateAllId[args.Id] = time.Now()
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
	//log.Printf("%s tick", pb.me)

	vx, err := pb.vs.Ping(pb.curView.Viewnum)
	if err != nil {
		fmt.Println(pb.me, err)
	}

	//log.Printf("vx Primary is %s", vx.Primary)
	//log.Printf("vx Backup is %s", vx.Backup)
	//log.Printf("vx Viewnum is %d", vx.Viewnum)

	if pb.me == vx.Primary && pb.curView.Backup != vx.Backup && vx.Backup != "" {
		pb.mu.Lock()
		defer pb.mu.Unlock()

		pb.curView = vx

		var args ReplicateAllArgs
		var reply ReplicateAllReply

		args.Me = pb.curView.Primary
		args.DB = make(map[string]string)
		args.LastPutAppendId = make(map[int64]time.Time)
		args.Id = nrand()
		args.AckedId = pb.ackedReplicateAllId

		for k, v := range pb.db {
			args.DB[k] = v
		}

		for k, v := range pb.lastPutAppendId {
			args.LastPutAppendId[k] = v
		}

		//log.Printf("call ReplicateAll")

		//log.Printf("Server tick curView.Backup is %s", pb.curView.Backup)

		c := make(chan bool, 1)
		for {
			go func() {
				c <- call(pb.curView.Backup, "PBServer.ReplicateAll", &args, &reply)
			}()
			succeed := <-c
			if succeed == false {
				vx, _ := pb.vs.Get()
				pb.curView = vx
				if pb.curView.Backup == "" || pb.curView.Primary != pb.me {
					return
				}
			} else {
				//log.Printf("Replication done\n")
				pb.ackedReplicateAllId = args.Id
				return
			}
		}
	} else if pb.curView != vx {
		pb.mu.Lock()
		defer pb.mu.Unlock()

		pb.curView = vx
	}
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
	pb.lastPutAppendId = make(map[int64]time.Time)
	pb.lastReplicateAllId = make(map[int64]time.Time)

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
