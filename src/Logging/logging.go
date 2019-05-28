package Logging

import (
	log "github.com/sirupsen/logrus"
	"os"
)

func init() {
	// log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
	log.SetReportCaller(false)

}

func GetLogger(moduleName, loggerName string) *log.Entry {
	return log.WithFields(log.Fields{"module": moduleName, "logger": loggerName})
}
