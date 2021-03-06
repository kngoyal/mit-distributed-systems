package mrconcurrent

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
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

// Add your RPC definitions here.

type Args struct{}

type Task struct {
	Which     string
	Name      string
	FileName  string
	Key       string
	Values    []string
	Result    string
	Available bool
	Pairs     []KeyValue
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-wc-"
	s += strconv.Itoa(os.Getuid())
	log.Debug("sockname: %s\n", s)
	return s
}
