package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
        args := TaskRequestArgs{}
        reply := TaskRequestReply{}
        
        if !call("Master.GetTask", &args, &reply) {
            break
        }

		switch reply.TaskType {
        case MapTask:
            performMapTask(mapf, reply.Filename, reply.TaskID, reply.NReduce)
            // reportTaskCompletion
        case ReduceTask:
            // performReduceTask
            // reportTaskCompletion
        case WaitTask:
            time.Sleep(1 * time.Second)
        case ExitTask:
            return
        }
	}
}

func performMapTask(mapf func(string, string) []KeyValue, filename string, mapID int, nReduce int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	// Partition results by reduce bucket
    partitions := make([][]KeyValue, nReduce)
    for _, kv := range kva {
        p := ihash(kv.Key) % nReduce
        partitions[p] = append(partitions[p], kv)
    }

	// Write partitions to temp files
    for i := 0; i < nReduce; i++ {
        tempFile, _ := ioutil.TempFile("", "mr-tmp-*")
        // JSON encode results
        enc := json.NewEncoder(tempFile)
        for _, kv := range partitions[i] {
            enc.Encode(&kv)
        }
        tempFile.Close()
        outFile := fmt.Sprintf("mr-%d-%d", mapID, i)
        os.Rename(tempFile.Name(), outFile)
    }
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
