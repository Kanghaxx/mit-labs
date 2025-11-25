package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		var id, taskType, ok = GetTask()
		if !ok {
			fmt.Printf("RPC call to get task failed. Exiting.\n")
			return
		}
		switch taskType {
		case MapTask:
			DebugLog("Starting Map Task %v\n", id)
			var filename, nReduce = GetMapTask(id)
			DebugLog("Map Task %v assigned file %v. nReduce=%v\n", id, filename, nReduce)

			executeMap(id, filename, nReduce, mapf)

			CompleteMapTask(id)
			DebugLog("Map Task completed. id=%v\n", id)

		case ReduceTask:
			DebugLog("Starting Reduce Task %v\n", id)
			var filename = GetReduceTask(id)
			DebugLog("Reduce Task %v assigned file %v", id, filename)

			executeReduce(id, filename, reducef)

			CompleteReduceTask(id)
			DebugLog("Reduce Task completed. id=%v\n", id)

		case NoneTask:
			DebugLog("No Task assigned.\n")
		}
		time.Sleep(1 * time.Second)
	}
}

func executeReduce(id int, filename string, reducef func(string, []string) string) bool {
	filenames := findFilesMyMask(id)
	intermediate := loadFilesToKV(filenames)
	sort.Sort(ByKey(intermediate))
	results := reduce(intermediate, reducef)
	saveToOutputFile(results, id)
	return true
}

func saveToOutputFile(results []KeyValue, id int) {
	// print the result to mr-out-0.
	tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-tmp-%v-*", id))
	if err != nil {
		log.Fatalf("Reduce: cannot create temp file: %v", err)
	}
	for _, output := range results {
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", output.Key, output.Value)
	}
	// Ensure data is flushed and file is closed before making it visible
	if err := tempFile.Sync(); err != nil {
		// If sync fails, still attempt to close and proceed to rename; fail loudly
		tempFile.Close()
		log.Fatalf("Reduce task %v: Cannot sync temp file: %v", id, err)
	}
	if err := tempFile.Close(); err != nil {
		log.Fatalf("Reduce task %v: Cannot close temp file: %v", id, err)
	}

	if err := os.Rename(tempFile.Name(), "mr-out-"+strconv.Itoa(id)); err != nil {
		log.Fatalf("Reduce task %v: Cannot rename temp file: %v", id, err)
	}
}

func reduce(intermediate []KeyValue, reducef func(string, []string) string) []KeyValue {
	// call Reduce on each distinct key in intermediate[]
	var outputs = []KeyValue{}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		result := reducef(intermediate[i].Key, values)
		outputs = append(outputs, KeyValue{intermediate[i].Key, result})

		i = j
	}
	return outputs
}

func loadFilesToKV(filenames []string) []KeyValue {
	//fmt.Println("Reducing files:", filenames)
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			//fmt.Printf("Reduce task id = %v. Read from file:%v\n", bucketN, kv)
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	return intermediate
}

func findFilesMyMask(bucketN int) []string {
	mask := fmt.Sprintf("mr-int-%v-*", bucketN)
	filenames, err := filepath.Glob(mask)
	if err != nil {
		fmt.Println("Reduce error:", err)
		return nil
	}
	return filenames
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getBucket(key string, nReduce int) int {
	return ihash(key) % nReduce
}

func executeMap(id int, filename string, nReduce int,
	mapf func(string, string) []KeyValue) bool {
	content := readInputMapFile(filename)
	mapResult := mapf(filename, string(content))
	buckets := arrangeToBuckets(mapResult, nReduce)
	saveToIntermediateFiles(buckets, nReduce, id)
	return true
}

func readInputMapFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

func arrangeToBuckets(kva []KeyValue, nReduce int) [][]KeyValue {
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		bucket := getBucket(kv.Key, nReduce)
		buckets[bucket] = append(buckets[bucket], kv)
	}
	return buckets
}

func saveToIntermediateFiles(buckets [][]KeyValue, nReduce int, id int) {
	for i, _ := range buckets {
		// create temp file in the current directory to ensure rename stays on same FS
		f, err := os.CreateTemp(".", fmt.Sprintf("mr-tmp-%v-*", id))
		if err != nil {
			log.Fatalf("Cannot create temp file: %v", err)
		}
		enc := json.NewEncoder(f)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("Map task %v: Cannot encode KeyValue %v: %v", id, kv, err)
			}
		}
		// Ensure data is flushed and file is closed before making it visible
		if err := f.Sync(); err != nil {
			// If sync fails, still attempt to close and proceed to rename; fail loudly
			f.Close()
			log.Fatalf("Map task %v: Cannot sync temp file: %v", id, err)
		}
		if err := f.Close(); err != nil {
			log.Fatalf("Map task %v: Cannot close temp file: %v", id, err)
		}
		if err := os.Rename(f.Name(), fmt.Sprintf("mr-int-%v-%v", i, id)); err != nil {
			log.Fatalf("Map task %v: Cannot rename temp file: %v", id, err)
		}
	}
}

func GetTask() (int, TaskType, bool) {
	args := GetTaskArgs{}

	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return reply.Id, reply.TaskType, true
	} else {
		fmt.Printf("call failed!\n")
		return -1, 0, false
	}
}

func GetMapTask(id int) (string, int) {
	args := GetTaskArgs{}
	args.Id = id

	reply := GetMapTaskReply{}

	ok := call("Coordinator.GetMapTask", &args, &reply)
	if ok {
		return reply.Filename, reply.NReduce
	} else {
		fmt.Printf("call failed!\n")
		return "", 0
	}
}

func GetReduceTask(id int) string {
	args := GetTaskArgs{}
	args.Id = id

	reply := GetReduceTaskReply{}

	ok := call("Coordinator.GetReduceTask", &args, &reply)
	if ok {
		return reply.Filename
	} else {
		fmt.Printf("call failed!\n")
		return ""
	}
}

func CompleteMapTask(id int) bool {
	args := CompleteTaskArgs{}
	args.Id = id

	reply := CompleteTaskReply{}

	ok := call("Coordinator.CompleteMapTask", &args, &reply)
	if ok {
		return reply.Ok
	} else {
		fmt.Printf("call failed!\n")
		return false
	}
}

func CompleteReduceTask(id int) bool {
	args := CompleteTaskArgs{}
	args.Id = id

	reply := CompleteTaskReply{}

	ok := call("Coordinator.CompleteReduceTask", &args, &reply)
	if ok {
		return reply.Ok
	} else {
		fmt.Printf("Coordinator.CompleteReduceTask call failed! task id=%v \n", args.Id)
		return false
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
