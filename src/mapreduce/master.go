package mapreduce

import "container/list"
import "fmt"
import "time"
import "log"

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
	go mr.workRegister()

	var (
		mreply chan DoJobReply = make(chan DoJobReply, mr.nMap)
		rreply chan DoJobReply = make(chan DoJobReply, mr.nReduce)
		mJob   int             = 0
		rJob   int             = 0
	)
	// map
	for mJob < mr.nMap {
		Worker := mr.GetAvailableWoker()
		if Worker == nil {
			time.Sleep(time.Microsecond * 100)
		} else {
			go mr.job(Worker.address, mJob, Map, mreply)
			mJob += 1
		}
	}

	// waiting for all map jobs done
	for i := 0; i < mr.nMap; i++ {
		r := <-mreply
		log.Printf("DoMap Done: %t\n", r.OK)
	}

	// reduce
	for rJob < mr.nReduce {
		Worker := mr.GetAvailableWoker()
		if Worker == nil {
			time.Sleep(time.Microsecond * 100)
		} else {
			go mr.job(Worker.address, rJob, Reduce, rreply)
			rJob += 1
		}
	}

	// waiting for all reduce jobs done
	for i := 0; i < mr.nReduce; i++ {
		r := <-rreply
		log.Printf("DoReduce Done: %t", r.OK)
	}

	return mr.KillWorkers()
}

func (mr *MapReduce) job(address string, jobNumber int, operation JobType, resp chan DoJobReply) {
	args := &DoJobArgs{
		File:      mr.file,
		JobNumber: jobNumber,
		Operation: operation,
	}
	if args.Operation == Map {
		args.NumOtherPhase = mr.nReduce
	} else {
		args.NumOtherPhase = mr.nMap
	}
	var reply DoJobReply
	//	fmt.Printf("Before call:%s,%#v\n", worker.address, *args)
	if ok := call(address, "Worker.DoJob", args, &reply); ok {
		fmt.Printf("DoWork: RPC %s %s result: %t\n", address, args.Operation, reply.OK)
		mr.SetAvailableWoker(address)
	}

	resp <- reply
}

func (mr *MapReduce) workRegister() {
	for mr.alive {
		select {
		case worker := <-mr.registerChannel:
			if _, ok := mr.Workers[worker]; ok {
				fmt.Printf("worker: %s aready registed!", worker)
			} else {
				fmt.Printf("new worker: %s", worker)
				mr.Workers[worker] = &WorkerInfo{
					address: worker,
				}

				mr.SetAvailableWoker(worker)
			}
		}
	}
}

// set available worker by address
// TODO keep Set and Get has simillar parameter and return
func (mr *MapReduce) SetAvailableWoker(address string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if worker, ok := mr.Workers[address]; ok {
		mr.aWorkers = append(mr.aWorkers, worker)
	}
}

// get available worker
func (mr *MapReduce) GetAvailableWoker() *WorkerInfo {
	mr.mu.Lock()
	defer mr.mu.Unlock()

	if length := len(mr.aWorkers); length > 0 {
		worker := mr.aWorkers[length-1]
		mr.aWorkers = mr.aWorkers[:length-1]
		return worker
	} else {
		return nil
	}
}
