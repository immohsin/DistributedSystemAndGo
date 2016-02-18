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
	curView      View
	nextView     View
	curViewAcked bool
	cntPrimary   int
	cntBackup    int
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	// By Yan
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if atomic.LoadInt32(&vs.cntBackup) >= 5 {
		atomic.StoreInt32(&vs.cntPrimary, 0)
		vs.nextView.Backup = nil
		vs.nextView.Viewnum++
	}
	if atomic.LoadInt32(&vs.cntPrimary) >= 5 {
		atomic.StoreInt32(&vs.cntPrimary, 0)
		vs.nextView.Primary = vs.nextView.Backup
		vs.nextView.Viewnum++
	}
	if vs.curView.Primary == nil {
		vs.nextViewAcked = false
		vs.nextView.Viewnum++
		vs.nextView.Primary = args.Me
		atomic.StoreInt32(&vs.cntPrimary, 0)
		if vs.curView.Viewnum == 1 {
			vs.nextViewAcked = true
		}
	} else if vs.curView.Backup == nil && vs.curView.Primary != args.Me {
		vs.nextView = vs.curView
		vs.nextView.Viewnum++
		vs.nextView.Backup = args.Me
		atomic.StoreInt32(&vs.cntPrimary, 0)
	} else if vs.curView.Primary == args.Me {
		atomic.StoreInt32(&vs.cntPrimary, 0)
		if vs.curView.Viewnum == args.Viewnum {
			vs.curViewAcked = true
		}
	} else if vs.curView.Backup == args.Me {
		atomic.StoreInt32(&vs.cntBackup, 0)
	}

	if vs.curViewAcked {
		vs.curView = vs.nextView
		vs.curViewAcked = false
	}
	reply.View = vs.curView

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

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
	atomic.AddInt32(&vs.cntPrimary, 1)
	atomic.AddInt32(&vs.cntBackup, 1)
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
	vs.curView.Primary = nil
	vs.curView.Backup = nil
	vs.curView.Viewnum = 0
	vs.nextView = vs.curView
	vs.curViewAcked = false
	vs.cntPrimary = 0
	vs.cntBackup = 0

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
