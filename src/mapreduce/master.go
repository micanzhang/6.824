package mapreduce

import "container/list"
import "fmt"
import "log"
import "sync"

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
	)
	// map
	for mr.jobs.Length() > 0 {
		var worker *WorkerInfo
		for worker == nil {
			worker = mr.GetAvailableWoker()
		}
		if mJob, ok := mr.jobs.Pop(); ok {
			go mr.job(worker.address, mJob, Map, mreply)
		}
	}

	// waiting for all map jobs done
	for mr.doneJobs < mr.nMap {
		r := <-mreply
		if r.OK {
			log.Printf("DoMap Done: %t\n", r.OK)
		}
	}

	// reduce
	mr.doneJobs = 0
	mr.jobs = InitJobStack(mr.nReduce)
	for mr.jobs.Length() > 0 {
		var worker *WorkerInfo
		for worker == nil {
			worker = mr.GetAvailableWoker()
		}
		if rJob, ok := mr.jobs.Pop(); ok {
			go mr.job(worker.address, rJob, Reduce, rreply)
		}
	}

	// waiting for all reduce jobs done
	for mr.doneJobs < mr.nReduce {
		r := <-rreply
		if r.OK {
			log.Printf("DoReduce Done: %t", r.OK)
		}
	}

	return mr.KillWorkers()
}

func (mr *MapReduce) job(address string, jobNumber int, operation JobType, resp chan DoJobReply) {
	var reply DoJobReply

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
	//	fmt.Printf("Before call:%s,%#v\n", worker.address, *args)
	if ok := call(address, "Worker.DoJob", args, &reply); ok {
		fmt.Printf("DoWork: RPC %s %s result: %t\n", address, args.Operation, reply.OK)
		mr.SetAvailableWoker(address)
		if reply.OK == false {
			log.Printf("DoWork failed: %d\n", jobNumber)
		} else {
			mr.doneJobs += 1
		}
	}

	// redo when job failed
	if reply.OK == false {
		mr.jobs.Push(jobNumber)
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

type JobStack struct {
	data []int
	mu   sync.Mutex
}

func (jq *JobStack) Push(elem int) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	jq.data = append(jq.data, elem)
}

func (jq *JobStack) Pop() (elem int, ok bool) {
	length := jq.Length()
	if length <= 0 {
		return
	}

	jq.mu.Lock()
	defer jq.mu.Unlock()

	elem = jq.data[length-1]
	jq.data = jq.data[:length-1]
	ok = true

	return
}

func (jq *JobStack) Length() int {
	return len(jq.data)
}

func InitJobStack(n int) *JobStack {
	jq := &JobStack{
		data: make([]int, n),
	}
	for i := 0; i < n; i++ {
		jq.Push(i)
	}

	return jq
}
