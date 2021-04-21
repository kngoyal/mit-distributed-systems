package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Args struct{}

type Task struct {
	Which    string
	FileName string
	Key      string
	Values   []string
	Result   string
	Done     bool
	Failed   bool
	Pairs    []KeyValue
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-wc-"
	s += strconv.Itoa(os.Getuid())
	fmt.Printf("sockname: %s\n", s)
	return s
}