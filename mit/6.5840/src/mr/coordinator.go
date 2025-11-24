package mr

import (
	"container/list"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
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
	activeTasksChannel    chan int
	completedTasksChannel chan int
	phazeCompleted        chan bool
	totalTasks            int
	timeout               time.Duration
	activeQueue           *list.List
	completedTasks        map[int]bool
	completedTasksCount   int
	completedMu           sync.Mutex
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
	for id, _ := range filesMap {
		pendingMapTasks <- id
	}

	//var timeout = time.Millisecond * 500
	var timeout = time.Second * 10

	var d = &PhaseController{
		totalTasks:            len(files),
		timeout:               timeout,
		activeQueue:           list.New(),
		completedTasks:        make(map[int]bool),
		pendingTasksChannel:   pendingMapTasks,
		activeTasksChannel:    make(chan int, len(files)),
		completedTasksChannel: make(chan int, len(files)),
		phazeCompleted:        make(chan bool, len(files)),
		completedMu:           sync.Mutex{},
	}

	var r = &PhaseController{
		totalTasks:            nReduce,
		timeout:               timeout,
		activeQueue:           list.New(),
		completedTasks:        make(map[int]bool),
		pendingTasksChannel:   make(chan int, nReduce),
		activeTasksChannel:    make(chan int, nReduce),
		completedTasksChannel: make(chan int, nReduce),
		phazeCompleted:        make(chan bool, nReduce),
		completedMu:           sync.Mutex{},
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
		//reply.Filename = entry.Filepath
		reply.Filename = entry.Filename
		reply.NReduce = c.nReduce
	}
	//fmt.Printf("GetMapTask Request received. Filename=%v nReduce=%v\n", reply.Filename, reply.NReduce)
	return nil
}

func (c *Coordinator) GetReduceTask(args *GetTaskArgs, reply *GetReduceTaskReply) error {
	if entry, ok := c.filesMap[args.Id]; ok {
		//reply.Filename = entry.Filepath
		reply.Filename = entry.Filename
	} else {
		reply.Filename = ""
	}
	// Always provide the number of reduce tasks
	reply.NReduce = c.nReduce
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
	c.completedMu.Lock()
	defer c.completedMu.Unlock()
	return c.completedTasksCount == c.totalTasks
}

func (c *PhaseController) TaskCompleted(id int) {
	c.completedTasksChannel <- id
}

func (c *PhaseController) taskStarted(id int) {
	c.activeTasksChannel <- id
}

func (c *PhaseController) Start() {
	go func() {
		for {
			for notMore := false; !notMore; {
				select {
				case id := <-c.activeTasksChannel:
					c.activeQueue.PushBack(TaskState{Id: id, StartedAt: time.Now().UTC()})
				default:
					notMore = true
				}
			}

			for notMore := false; !notMore; {
				select {
				case id := <-c.completedTasksChannel:
					_, ok := c.completedTasks[id]
					if !ok {
						c.completedMu.Lock()
						c.completedTasksCount++
						c.completedMu.Unlock()
						if Debug {
							fmt.Printf("Task completed. Id=%v CompletedCount=%v \n", id, c.completedTasksCount)
						}
					}
					c.completedTasks[id] = true
				default:
					notMore = true
				}
			}

			if c.IsCompleted() {
				c.phazeCompleted <- true
				fmt.Printf("Stage completed. Total completed tasks=%v\n", c.completedTasksCount)
				return
			}

			for {
				oldest := c.activeQueue.Front()
				if oldest == nil {
					break
				}
				oldestTask := oldest.Value.(TaskState)
				_, ok := c.completedTasks[oldestTask.Id]
				if ok {
					c.activeQueue.Remove(oldest)
					continue
				}
				elapsed := time.Now().UTC().Sub(oldestTask.StartedAt)
				if elapsed <= c.timeout {
					break
				}
				fmt.Printf("Re-queueing stuck task Id=%v startedAt=%v \n", oldestTask.Id, oldestTask.StartedAt)
				c.pendingTasksChannel <- oldestTask.Id
				c.activeQueue.Remove(oldest)
			}

			time.Sleep(time.Millisecond * 200)
		}
	}()
}

func (c *Coordinator) handleMapPhazeCompletion() {
	go func() {
		<-c.mapContoller.phazeCompleted
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
