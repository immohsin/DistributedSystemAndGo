package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	curView                View
	nextView               View
	primaryAcked           bool
	nextViewExists         bool
	primaryFailureDetected bool
	timeTable              map[string]time.Time
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	// By Yan
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.timeTable[args.Me] = time.Now()

	if vs.curView.Primary == "" {
		vs.curView.Primary = args.Me
		vs.curView.Viewnum++
	} else if vs.curView.Backup == "" && vs.curView.Primary != args.Me {
		vs.nextView = vs.curView
		vs.nextView.Backup = args.Me
		vs.nextViewExists = true
	} else if vs.curView.Primary == args.Me {
		if vs.curView.Viewnum == args.Viewnum {
			vs.primaryAcked = true
			if !vs.nextViewExists {
				vs.nextView = vs.curView
			} else {
				vs.nextView.Viewnum++
			}
		} else {
			if !vs.primaryFailureDetected {
				vs.curView.Primary = vs.curView.Backup
				vs.curView.Backup = ""
				vs.curView.Viewnum++
			} else {
				vs.primaryFailureDetected = false
			}
		}
	} else if vs.curView.Backup == args.Me {
		if vs.curView.Viewnum == args.Viewnum {

		} else {
			/*if vs.nextView == nil {
				vs.nextView = vs.curView
				vs.nextView.Backup = ""
			}*/
		}
	}

	if vs.primaryAcked {
		vs.curView = vs.nextView
		vs.primaryAcked = false
		vs.nextViewExists = false
	}
	reply.View = vs.curView
	log.Printf("reply Primary %s\n", reply.View.Primary)
	log.Printf("reply Backup %s\n", reply.View.Backup)
	log.Printf("reply Viewnum %d\n", reply.View.Viewnum)

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	// By Yan
	reply.View = vs.curView

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	// By Yan
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// Primary dead, Backup alive
	// Primary alive, Backup dead
	// Primary and Backup dead
	if vs.curView.Backup != "" {
		timediff := time.Since(vs.timeTable[vs.curView.Backup])
		if timediff >= DeadPings*PingInterval {
			//Backup dead
			vs.nextView = vs.curView
			vs.nextView.Backup = ""
		}
	}
	if vs.curView.Primary != "" {
		timediff := time.Since(vs.timeTable[vs.curView.Primary])
		if timediff >= DeadPings*PingInterval {
			//Primary dead
			vs.primaryFailureDetected = true
			vs.curView.Primary = vs.curView.Backup
			vs.curView.Backup = ""
			vs.curView.Viewnum++
		}
	}

}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	// By yan
	vs.curView.Primary = ""
	vs.curView.Backup = ""
	vs.curView.Viewnum = 0
	vs.nextView = vs.curView
	vs.primaryAcked = false
	vs.nextViewExists = false
	vs.primaryFailureDetected = false
	vs.timeTable = make(map[string]time.Time)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
