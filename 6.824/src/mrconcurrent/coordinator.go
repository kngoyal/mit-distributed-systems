package mrconcurrent

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"

	log "github.com/sirupsen/logrus"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Coordinator struct {
	// Your definitions here.
	tasks          chan Task
	pairs          []KeyValue
	reduceReady    bool
	nReduce        int
	timeOut        int
	outputFileName string
	outputFile     *os.File
	terminate      bool
	mu             sync.Mutex
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

func (c *Coordinator) CreateReduceTasks() {
	sort.Sort(ByKey(c.pairs))

	i := 0
	for i < len(c.pairs) {
		j := i + 1

		for j < len(c.pairs) && c.pairs[i].Key == c.pairs[j].Key {
			j++
		}

		task := Task{}
		task.Which = "reduce"
		task.Name = "reduce" + "-" + strconv.Itoa(i)
		task.Key = c.pairs[i].Key
		task.Available = true
		task.Values = []string{}

		for k := i; k < j; k++ {
			task.Values = append(task.Values, c.pairs[k].Value)
		}

		c.tasks[task.Name] = task
		log.Debug("C: Task '%v' created\n", task.Name)

		i = j
	}

	c.outputFileName = "mr-out-concurrent"
	c.outputFile, _ = os.Create(c.outputFileName)

	close(c.tasks)
}

func (c *Coordinator) GiveTask(args *Args, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Debug("C: Preparing task for W %v\n", c.reduceReady)
	log.Debug("W: %d Tasks left\n", len(c.tasks))
	if !c.reduceReady {
		select {
		case task := <-c.tasks:
			if task.Available && task.Which == "map" {
				reply.Name = task.Name
				reply.Which = task.Which
				reply.FileName = task.FileName
				reply.Available = false
				// c.tasks[name] = *reply
				return nil
			}
		default:
			c.reduceReady = true
			c.CreateReduceTasks()
			reply.Which = "wait"
		}
	} else {
		for name, task := range c.tasks {
			if task.Available && task.Which == "reduce" {
				reply.Name = task.Name
				reply.Which = task.Which
				reply.Key = task.Key
				reply.Values = task.Values
				reply.Available = false
				// c.tasks[name] = *reply
				return nil
			}
		default:
			c.terminate = true
			reply.Which = "shutdown"
		}
	}
	return nil
}

func (c *Coordinator) TakePairs(args *Task, reply *Args) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pairs = append(c.pairs, args.Pairs...)

	log.Debug("C: Task '%v' finished\n", args.Name)
	delete(c.tasks, args.Name)
	return nil
}

func (c *Coordinator) ReceiveCount(args *Task, reply *Args) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	fmt.Fprintf(c.outputFile, "%v %v\n", args.Key, args.Result)

	log.Debug("C: Task '%v' finished\n", args.Name)
	delete(c.tasks, args.Name)
	if c.reduceReady && len(c.tasks) == 0 {
		c.terminate = true
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.terminate {
		c.outputFile.Close()
		return true
	}
	return false
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}


	c.tasks = make(map[string]Task)
	c.nReduce = nReduce
	for i, fileName := range files {
		task := Task{}
		task.Which = "map"
		task.FileName = fileName
		task.Available = true
		c.tasks[task.Which+"-"+strconv.Itoa(i)] = task
	}
	log.Info("%T %v\n", c.tasks, c.tasks)

	c.server()
	return &c
}
