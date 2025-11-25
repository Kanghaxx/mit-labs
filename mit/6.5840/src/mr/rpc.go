package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type TaskType int

const (
	MapTask    TaskType = iota
	ReduceTask TaskType = iota
	NoneTask   TaskType = iota
)

type GetTaskReply struct {
	Id       int
	TaskType TaskType
}

type GetTaskArgs struct {
	Id int
}

type GetMapTaskReply struct {
	Filename string
	NReduce  int
}

type GetReduceTaskReply struct {
	Filename string
	NReduce  int
}

type CompleteTaskArgs struct {
	Id int
}
type CompleteTaskReply struct {
	Ok bool
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
