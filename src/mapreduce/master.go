package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	// By Yan
	var sem = make(chan *Worker, len(mr.Workers))
	// push all registered workers into our semaphore
	for _, wr := range mr.Workers {
		sem <- wr
	}
	for i := 0; i < mr.nMap; i++ {
		wr := <-sem
		go func() {
			args := &DoJobArgs{}
			args.File = mr.file
			args.Operation = Map
			args.JobNumber = i
			args.NumOtherPhase =  mr.nReduce
			var reply DoJobReply
			ok := call(wr.address, "Worker.DoJob", args, &reply)
			if ok == false {
				fmt.Printf("Map: RPC %s register error\n", wr.address)
			}
			sem <- wr
		}()
	}
	for i := 0; i < mr.nReduce; i++ {
		wr := <-sem
		go func() {
			args := &DoJobArgs{}
			args.File = mr.file
			args.Operation = Reduce
			args.JobNumber = i
			args.NumOtherPhase =  mr.nMap 
			var reply DoJobReply
			ok := call(wr.address, "Worker.DoJob", args, &reply)
			if ok == false {
				fmt.Printf("Deduce: RPC %s register error\n", wr.address)
			}
			sem <- wr
		}()
	} 
	return mr.KillWorkers()
}
