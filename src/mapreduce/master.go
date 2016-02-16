package mapreduce

import "container/list"
import "fmt"
import "sync"
//import "log"

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
	done := make(chan struct{})
	workersChannel := make(chan string)
	
	//grab info from register channel
	go func() {
		for {
			select {
				case registeredWorker := <- mr.registerChannel:
					_, exist := mr.Workers[registeredWorker]
					if !exist {
						var wi WorkerInfo
						wi.address = registeredWorker
						mr.Workers[registeredWorker] = &wi
					}
					workersChannel <- registeredWorker
				case <-done:
					return
			}
		}
	}()

	mr.ScheduleMap(done, workersChannel)
	mr.ScheduleReduce(done, workersChannel)
	close(done)

	return mr.KillWorkers()
}

func (mr *MapReduce) ScheduleMap(done <-chan struct{}, workersChannel chan string) {
	mapTasks := make(chan int, mr.nMap)
	mapDone := make(chan struct{})
	var mutex = &sync.Mutex{}

	for i := 0; i < mr.nMap; i++ {
		mapTasks <- i
	}

	for {
		select {
			case i := <- mapTasks:
				wrName := <-workersChannel
				go func() {
					args := &DoJobArgs{}
					args.File = mr.file
					args.Operation = Map
					args.JobNumber = i
					args.NumOtherPhase = mr.nReduce
					var reply DoJobReply
					
					ok := call(wrName, "Worker.DoJob", args, &reply)
					if ok == false {
						fmt.Printf("Map: RPC %s register error\n", wrName)
						mapTasks <- i
					} else {
						mutex.Lock()
						
						mr.cntMap--
						if mr.cntMap == 0 {
							close(mapDone)
						}
						mutex.Unlock()
						select {
							case workersChannel <- wrName:
							case <-done:
								return
						}
					}
				}()
			case <-mapDone:
				return
		}
	}
}

func (mr *MapReduce) ScheduleReduce(done <-chan struct{}, workersChannel chan string) {
	reduceTasks := make(chan int, mr.nReduce)
	reduceDone := make(chan struct{})
	var mutex = &sync.Mutex{}

	for i := 0; i < mr.nReduce; i++ {
		reduceTasks <- i
	}

	for {
		select {
			case i := <- reduceTasks:
				wrName := <-workersChannel
				go func() {
					args := &DoJobArgs{}
					args.File = mr.file
					args.Operation = Reduce
					args.JobNumber = i
					args.NumOtherPhase = mr.nMap
					var reply DoJobReply

					ok := call(wrName, "Worker.DoJob", args, &reply)
					if ok == false {
						fmt.Printf("Map: RPC %s register error\n", wrName)
						reduceTasks <- i
					} else {
						mutex.Lock()
						mr.cntReduce--
						if mr.cntReduce == 0 {
							close(reduceDone)
						}
						mutex.Unlock()
						select {
							case workersChannel <- wrName:
							case <-done:
								return
						}
					}
				}()
			case <-reduceDone:
				return
		}
	}
}