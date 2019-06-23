package Logging

import (
	runtime "github.com/banzaicloud/logrus-runtime-formatter"
	log "github.com/sirupsen/logrus"
	"os"
	"strings"
)

func init() {
	// log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&runtime.Formatter{ChildFormatter: &log.JSONFormatter{}, Line: true, Package: true})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.InfoLevel)
	log.SetReportCaller(false)

}

func SetLogLevel(level string) {
	switch strings.ToUpper(level) {
	case "TRACE":
		log.SetLevel(log.TraceLevel)
	case "DEBUG":
		log.SetLevel(log.DebugLevel)
	case "INFO":
		log.SetLevel(log.InfoLevel)
	case "WARN":
		log.SetLevel(log.WarnLevel)
	case "ERROR":
		log.SetLevel(log.ErrorLevel)
	case "FATAL":
		log.SetLevel(log.FatalLevel)
	case "PANIC":
		log.SetLevel(log.PanicLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}
}

func GetLogger(loggerName string) *log.Entry {
	return log.WithFields(log.Fields{"logger": loggerName})
}
