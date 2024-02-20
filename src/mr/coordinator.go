package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type CoordinatorState int

const (
	CoordinatorStateMap CoordinatorState = iota
	CoordinatorStateReduce
	CoordinatorStateFinish
)

type Coordinator struct {
	// Your definitions here.
	mutex       sync.Mutex
	nReduce     int
	mapWorks    []Work
	reduceWorks []Work
	state       CoordinatorState
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) checkAndUpdate() {
	if c.state == CoordinatorStateMap || c.state == CoordinatorStateReduce {
		var nextState CoordinatorState
		var works []Work
		if c.state == CoordinatorStateMap {
			nextState = CoordinatorStateReduce
			works = c.mapWorks
		} else {
			nextState = CoordinatorStateFinish
			works = c.reduceWorks
		}
		isFinish := true
		current := time.Now()
		for idx, work := range works {
			if work.State != WorkStateFinished {
				isFinish = false
			}
			if work.State == WorkStateDispatched && current.After(work.UpdateTime.Add(WorkFailInterval)) {
				works[idx].updateState(WorkStateInit)
			}
		}
		if isFinish {
			c.state = nextState
		}
	}
}

func (c *Coordinator) dispatchTask() Task {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.state == CoordinatorStateMap || c.state == CoordinatorStateReduce {
		var works []Work
		if c.state == CoordinatorStateMap {
			works = c.mapWorks
		} else {
			works = c.reduceWorks
		}
		for idx := range works {
			if works[idx].State == WorkStateInit {
				works[idx].updateState(WorkStateDispatched)
				return Task{TaskTypWork, works[idx], c.nReduce}
			}
		}
		return Task{TaskTypWait, Work{}, c.nReduce}
	} else {
		return Task{TaskTypQuit, Work{}, c.nReduce}
	}
}
func (c *Coordinator) finishWorkByTypIdx(typ WorkTyp, idx int) {
	var works []Work
	if typ == WorkTypMap {
		works = c.mapWorks
	} else {
		works = c.reduceWorks
	}
	works[idx].updateState(WorkStateFinished)

}

func (c *Coordinator) RequestTask(_ *Ignore, reply *Task) error {
	c.checkAndUpdate()
	task := c.dispatchTask()
	reply.Typ = task.Typ
	reply.Work = task.Work
	reply.NReduce = task.NReduce
	return nil
}

func (c *Coordinator) FinishTask(args *Task, _ *Ignore) error {
	c.finishWorkByTypIdx(args.Work.Typ, args.Work.Idx)
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.state == CoordinatorStateFinish
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	c := Coordinator{
		nReduce:     nReduce,
		mapWorks:    make([]Work, nMap),
		reduceWorks: make([]Work, nReduce),
		state:       CoordinatorStateMap,
	}
	for idx := range c.mapWorks {
		c.mapWorks[idx] = Work{
			Idx:        idx,
			State:      WorkStateInit,
			Typ:        WorkTypMap,
			UpdateTime: time.Now(),
			Inputs:     []string{files[idx]},
		}
	}
	for idx := range c.reduceWorks {
		c.reduceWorks[idx] = Work{
			Idx:        idx,
			State:      WorkStateInit,
			Typ:        WorkTypReduce,
			UpdateTime: time.Now(),
			Inputs:     getReduceInputs(nMap, idx),
		}
	}
	c.server()
	return &c
}

func getReduceInputs(nMap int, idx int) []string {
	inputs := make([]string, nMap)
	for i := range inputs {
		inputs[i] = fmt.Sprintf("mr-%v-%v", i, idx)
	}
	return inputs
}
