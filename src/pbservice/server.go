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
	lastId  int64
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	// By Yan
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.lastId == args.Id {
		reply.Err = OK
		return nil
	} else {
		pb.lastId = args.Id
	}

	//log.Printf("Get %s\n", pb.me)
	if pb.me != pb.curView.Primary {
		return errors.New(ErrWrongServer)
	}

	v, exist := pb.db[args.Key]
	if !exist {
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Err = OK
		reply.Value = v
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {

	// Your code here.
	// By Yan
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.lastId == args.Id {
		reply.Err = OK
		return nil
	} else {
		pb.lastId = args.Id
	}

	//log.Printf("PutAppend\n")
	if pb.me != pb.curView.Primary {
		return errors.New(ErrWrongServer)
	}

	if args.Op == "Put" {
		pb.db[args.Key] = args.Value
	} else {
		pb.db[args.Key] = pb.db[args.Key] + args.Value
	}

	var replyU PutAppendReply
	call(pb.curView.Backup, "PBServer.Forward", args, &replyU)

	reply.Err = OK

	return nil
}

// By Yan
func (pb *PBServer) Forward(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	//log.Printf("Forward\n")
	if pb.me != pb.curView.Backup || pb.me == pb.curView.Primary {
		return errors.New(ErrWrongServer)
	}
	//log.Printf("Forward 2\n")
	if args.Op == "Put" {
		pb.db[args.Key] = args.Value
	} else {
		pb.db[args.Key] = pb.db[args.Key] + args.Value
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
		var args ReplicateAllArgs
		var reply ReplicateAllReply

		args.DB = make(map[string]string)

		for k, v := range pb.db {
			args.DB[k] = v
		}

		//log.Printf("call ReplicateAll")
		succeed := false
		for succeed != true {
			succeed = call(vx.Backup, "PBServer.ReplicateAll", &args, &reply)
		}
	}

	pb.curView = vx

	//return reply.View, nil
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
