package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

var Debug = false

type FileInfo struct {
	Filename string
	Filepath string
}

type TaskState struct {
	Id        int
	StartedAt time.Time
}

type Coordinator struct {
	files            []string
	filesMap         map[int]FileInfo
	nReduce          int
	mapContoller     *PhaseController
	reduceController *PhaseController
}

type PhaseController struct {
	pendingTasksChannel   chan int
	completedTasksChannel chan int
	expiredTasksChannel   chan int
	phaseCompleted        chan bool
	totalTasks            int
	timeout               time.Duration
	completedTasks        map[int]bool
	activeTasks           map[int]bool
	completedTasksCount   int32
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var id = 0
	var filesMap = make(map[int]FileInfo)
	for _, file := range files {
		filepath, _ := filepath.Abs(file)
		filesMap[id] = FileInfo{
			Filename: file,
			Filepath: filepath,
		}
		id++
	}
	var pendingMapTasks = make(chan int, len(files))
	for id := range filesMap {
		pendingMapTasks <- id
	}

	//var timeout = time.Millisecond * 500
	var timeout = time.Second * 10

	var d = &PhaseController{
		totalTasks:            len(files),
		timeout:               timeout,
		completedTasks:        make(map[int]bool),
		activeTasks:           make(map[int]bool),
		pendingTasksChannel:   pendingMapTasks,
		completedTasksChannel: make(chan int, len(files)),
		expiredTasksChannel:   make(chan int, len(files)),
		phaseCompleted:        make(chan bool, len(files)),
	}

	var r = &PhaseController{
		totalTasks:            nReduce,
		timeout:               timeout,
		completedTasks:        make(map[int]bool),
		activeTasks:           make(map[int]bool),
		pendingTasksChannel:   make(chan int, nReduce),
		completedTasksChannel: make(chan int, nReduce),
		expiredTasksChannel:   make(chan int, nReduce),
		phaseCompleted:        make(chan bool, nReduce),
	}

	c := Coordinator{
		files:            files,
		filesMap:         filesMap,
		nReduce:          nReduce,
		mapContoller:     d,
		reduceController: r,
	}

	c.mapContoller.Start()
	c.reduceController.Start()
	c.handleMapPhazeCompletion()
	c.server()
	return &c
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.reduceController.IsCompleted()
}

// RPC handlers for the worker to call
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if !c.mapContoller.IsCompleted() {
		id := c.mapContoller.GetPendingTask()
		if id != -1 {
			reply.TaskType = MapTask
			reply.Id = id
			if Debug {
				fmt.Printf("GetTask Request received. Returning Map Task Id=%v\n", reply.Id)
			}
			return nil
		}
	} else {
		id := c.reduceController.GetPendingTask()
		if id != -1 {
			reply.TaskType = ReduceTask
			reply.Id = id
			if Debug {
				fmt.Printf("GetTask Request received. Returning Reduce Task Id=%v\n", reply.Id)
			}
			return nil
		}
	}
	reply.TaskType = NoneTask
	if Debug {
		fmt.Printf("GetTask Request received. No tasks available.\n")
	}
	return nil
}

func (c *Coordinator) GetMapTask(args *GetTaskArgs, reply *GetMapTaskReply) error {
	if entry, ok := c.filesMap[args.Id]; ok {
		reply.Filename = entry.Filename
		reply.NReduce = c.nReduce
	}
	//fmt.Printf("GetMapTask Request received. Filename=%v nReduce=%v\n", reply.Filename, reply.NReduce)
	return nil
}

func (c *Coordinator) GetReduceTask(args *GetTaskArgs, reply *GetReduceTaskReply) error {
	if entry, ok := c.filesMap[args.Id]; ok {
		reply.Filename = entry.Filename
	}
	//fmt.Printf("GetMapTask Request received. Filename=%v nReduce=%v\n", reply.Filename, reply.NReduce)
	return nil
}

func (c *Coordinator) CompleteMapTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.mapContoller.TaskCompleted(args.Id)
	reply.Ok = true
	//fmt.Printf("CompleteMapTask Request received. Id=%v\n", args.Id)
	return nil
}

func (c *Coordinator) CompleteReduceTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.reduceController.TaskCompleted(args.Id)
	reply.Ok = true
	//fmt.Printf("CompleteMapTask Request received. Id=%v\n", args.Id)
	return nil
}

func (c *PhaseController) AddTask(id int) {
	c.pendingTasksChannel <- id
}

func (c *PhaseController) GetPendingTask() int {
	if c.IsCompleted() {
		return -1
	}
	select {
	case id := <-c.pendingTasksChannel:
		c.taskStarted(id)
		return id
	default:
		return -1
	}
}

func (c *PhaseController) IsCompleted() bool {
	return int(atomic.LoadInt32(&c.completedTasksCount)) == c.totalTasks
}

func (c *PhaseController) TaskCompleted(id int) {
	c.completedTasksChannel <- id
}

func (c *PhaseController) taskStarted(id int) {
	go func() {
		time.Sleep(c.timeout)
		c.expiredTasksChannel <- id
	}()
}

func (c *PhaseController) Start() {
	go func() {
		for {
			select {
			case id := <-c.completedTasksChannel:
				_, ok := c.completedTasks[id]
				if !ok {
					newCount := atomic.AddInt32(&c.completedTasksCount, 1)
					if Debug {
						fmt.Printf("Task completed. Id=%v CompletedCount=%v \n", id, newCount)
					}
				} else {
					continue // zombie completion
				}
				c.completedTasks[id] = true

				if c.IsCompleted() {
					c.phaseCompleted <- true
					fmt.Printf("Stage completed. Total completed tasks=%v\n", c.completedTasksCount)
					return
				}
			case id := <-c.expiredTasksChannel:
				_, completed := c.completedTasks[id]
				if completed {
					continue // already completed, no need to reissue
				} else {
					c.pendingTasksChannel <- id
				}
			}
		}
	}()
}

func (c *Coordinator) handleMapPhazeCompletion() {
	go func() {
		<-c.mapContoller.phaseCompleted
		if Debug {
			fmt.Printf("Coordinator: Initializing Reduce stage...\n")
		}
		for id := 0; id < c.nReduce; id++ {
			c.reduceController.AddTask(id)
		}
		if Debug {
			fmt.Printf("Coordinator: Reduce stage initialized\n")
		}
	}()
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
