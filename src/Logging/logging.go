package Logging

import (
	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	log "github.com/sirupsen/logrus"
	"os"
)

func init() {
	// log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&runtime.Formatter{ChildFormatter: &log.JSONFormatter{}, Line: true, Package: true})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
	log.SetReportCaller(false)

}

func GetLogger(moduleName, loggerName string) *log.Entry {
	//return log.WithFields(log.Fields{"module": moduleName, "logger": loggerName})
	return log.WithFields(log.Fields{"logger": loggerName})
}
