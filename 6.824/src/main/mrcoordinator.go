package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"os"
	"time"

	logger "github.com/mit-distributed-systems/6.824/src/logger"
	mr "github.com/mit-distributed-systems/6.824/src/mr"
	log "github.com/sirupsen/logrus"
)

func main() {
	logger.SetLogLevel()
	if len(os.Args) < 2 {
		log.Error(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
