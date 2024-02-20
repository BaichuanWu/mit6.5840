package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

const WorkFailInterval = 10 * time.Second

type TaskTyp int
type WorkTyp int
type WorkState int

const (
	TaskTypQuit TaskTyp = iota
	TaskTypWait
	TaskTypWork
)

const (
	WorkStateInit WorkState = iota
	WorkStateDispatched
	WorkStateFinished
)

const (
	WorkTypMap WorkTyp = iota
	WorkTypReduce
)

type Work struct {
	Idx        int
	UpdateTime time.Time
	State      WorkState
	Typ        WorkTyp
	Inputs     []string
}

type Task struct {
	Typ     TaskTyp
	Work    Work
	NReduce int
}

type Ignore struct {
}

func (w *Work) updateState(state WorkState) {
	w.State = state
	w.UpdateTime = time.Now()
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
