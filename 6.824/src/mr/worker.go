package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	CallExample()
L:
	for {
		task := GetTask()
		fmt.Println(task)
		switch task.Which {
		case "map":
			file, err := os.Open(task.FileName)
			if err != nil {
				log.Fatalf("WORKER : cannot open '%v'", task.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("WORKER : cannot read '%v'", task.FileName)
			}
			file.Close()
			task.Pairs = mapf(task.FileName, string(content))
			PutPairs(task)
		case "reduce":
			task.Result = reducef(task.Key, task.Values)
			SendCount(task)
		case "wait":
			continue L
		}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func GetTask() Task {
	fmt.Println("W: Getting task from C")
	args := Args{}
	task := Task{}

	// send the RPC request, wait for the reply.
	call("Coordinator.GiveTask", &args, &task)

	// var task Task
	fmt.Printf("W: Received task '%v' from C\n", task.Name)

	return task
}

func PutPairs(task Task) {
	fmt.Printf("W: Sending '%v' task result to C\n", task.Name)
	args := Args{}

	// send the RPC request, wait for the reply.
	call("Coordinator.TakePairs", &task, &args)
}

func SendCount(task Task) {
	fmt.Printf("W: Sending '%v' task result to C\n", task.Name)
	args := Args{}

	// send the RPC request, wait for the reply.
	call("Coordinator.ReceiveCount", &task, &args)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
