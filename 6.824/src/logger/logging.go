package logger

import (
	"os"

	log "github.com/sirupsen/logrus"
)

func SetLogLevel() {
	lvl, ok := os.LookupEnv("GO_LOG_LEVEL")
	// LOG_LEVEL not set, let's default to info
	if !ok {
		lvl = "debug"
	}
	// parse string, this is built-in feature of logrus
	ll, err := log.ParseLevel(lvl)
	if err != nil {
		ll = log.DebugLevel
	}
	// set global log level
	log.SetLevel(ll)
}
