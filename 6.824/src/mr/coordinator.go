package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	tasks   []Task
	pairs   []KeyValue
	nReduce int
	timeOut int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GiveTask(args *Args, reply *Task) error {
	fmt.Println("Coordinator: Preparing task for Worker")
	for _, task := range c.tasks {
		if !task.Done {

			// enc := gob.NewEncoder(&reply.Message)
			// err := enc.Encode(task)
			// if err != nil {
			// 	fmt.Println("encode error:", err)
			// }
			// fmt.Printf("reply.Message : %T %v\n", reply.Message, reply.Message)

			fmt.Printf("task: %T %v\n", task, task)

			// reply = &task
			reply.Which = task.Which
			reply.FileName = task.FileName
			// reply.Pairs = task.Pairs
			reply.Key = task.Key
			reply.Values = task.Values
			reply.Result = task.Result
			reply.Done = task.Done
			reply.Failed = task.Failed

			fmt.Printf("reply : %T %v\n", reply, reply)

			return nil
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	for i, fileName := range files {
		task := Task{}
		task.Which = "map"
		task.FileName = fileName
		c.tasks = append(c.tasks, task)
		fmt.Printf("%d %T %v\n", i, task, task)
	}
	// Your code here.

	c.server()
	return &c
}
