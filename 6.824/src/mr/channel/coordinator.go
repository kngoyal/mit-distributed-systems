package mrchannel

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
	mapTasks       chan Task
	reduceTasks    chan Task
	pairs          []KeyValue
	reduceProgress bool
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
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Info("C: Creating Reduce tasks")

	i := 0
	go func() {
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

			log.Debugf("C: Task '%v' created", task.Name)

			c.reduceTasks <- task

			log.Debugf("C: Task '%v' out on channel", task.Name)

			i = j
		}

		log.Info("C: All Reduce tasks created")

		close(c.reduceTasks)
	}()
}

func (c *Coordinator) GiveTask(args *Args, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.reduceReady {
		log.Infof("C: Preparing MAP task for W, reduceReady: %v", c.reduceReady)
		select {
		case task := <-c.mapTasks:
			log.Debugf("C: Map Task found %v", task)
			if task.Available && task.Which == "map" {
				reply.Name = task.Name
				reply.Which = task.Which
				reply.FileName = task.FileName
				reply.Available = false
				return nil
			} else {
				log.Info("C: NO Map Task found")
				c.reduceReady = true
				c.reduceTasks = make(chan Task)
				sort.Sort(ByKey(c.pairs))
				go c.CreateReduceTasks()
				reply.Which = "wait"
				return nil
			}
		}
	} else {
		log.Infof("C: Preparing REDUCE task for W, reduceReady: %v", c.reduceReady)
		select {
		case task := <-c.reduceTasks:
			log.Debugf("C: Reduce Task found %v", task)
			if task.Available && task.Which == "reduce" {
				reply.Name = task.Name
				reply.Which = task.Which
				reply.Key = task.Key
				reply.Values = task.Values
				reply.Available = false
				return nil
			}
		}
	}
	reply.Which = "shutdown"
	c.terminate = true
	return nil
}

func (c *Coordinator) TakePairs(args *Task, reply *Args) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pairs = append(c.pairs, args.Pairs...)
	log.Debugf("C: Task '%v' finished", args.Name)
	return nil
}

func (c *Coordinator) ReceiveCount(args *Task, reply *Args) error {
	fmt.Fprintf(c.outputFile, "%v %v\n", args.Key, args.Result)
	log.Debugf("C: Task '%v' finished", args.Name)
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

	c.outputFileName = "mr-out-channel"
	c.outputFile, _ = os.Create(c.outputFileName)

	c.nReduce = nReduce
	c.mapTasks = make(chan Task)

	log.Info("C: Creating Map tasks")
	go func() {
		for i, fileName := range files {
			task := Task{}
			task.Which = "map"
			task.FileName = fileName
			task.Available = true
			task.Name = task.Which + "-" + strconv.Itoa(i)
			log.Debugf("%T %v", task, task)
			c.mapTasks <- task
		}
		log.Info("C: All Map tasks created")
		close(c.mapTasks)
	}()

	c.server()
	return &c
}
