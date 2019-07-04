package HealthCheck

import (
	"github.com/harshitandro/mongo-es-datasync/src/Configuration"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/harshitandro/mongo-es-datasync/src/Models/ConfigurationModels"
	"github.com/harshitandro/mongo-es-datasync/src/Models/DatabaseModels/CommonDatabaseModels"
	"github.com/sirupsen/logrus"
	"math"
	"runtime/debug"
	"sync"
	"time"
)

var logger *logrus.Entry
var healthcheck HealthCheck
var incrementDbRecordsGeneratedMutex sync.Mutex
var incrementDbRecordsProcessedMutex sync.Mutex
var incrementesRecordsStoredMutex sync.Mutex
var dataChannelRef *chan CommonDatabaseModels.OplogMessage
var lastOpTimestamp CommonDatabaseModels.LastOperation
var lastOpChannel *chan CommonDatabaseModels.LastOperation

var config *ConfigurationModels.ApplicationConfiguration

func init() {
	logger = Logging.GetLogger("Root")
}

func recoverPanic() {
	if r := recover(); r != nil {
		logger.WithField("trace", string(debug.Stack())).Errorf("Panic while printing healthcheck : %s", r)
	}
}

func EnableHealthCheck(dataChannel *chan CommonDatabaseModels.OplogMessage, lastOperationChannel *chan CommonDatabaseModels.LastOperation, applicationConfig *ConfigurationModels.ApplicationConfiguration) {
	dataChannelRef = dataChannel
	lastOpChannel = lastOperationChannel
	config = applicationConfig
	logger.Infoln("Starting healthcheck printing in every 10 seconds")
	go scheduleDisplayHealthCheck()
}

type HealthCheck struct {
	dbRecordsGeneratePassed int32
	dbRecordsGenerateFailed int32
	dbRecordsProcessPassed  int32
	dbRecordsProcessFailed  int32
	esRecordsStorePassed    int32
	esRecordsStoreFailed    int32
}

func IncrementDbRecordsGenerated(isPassed bool) {
	incrementDbRecordsGeneratedMutex.Lock()
	if isPassed {
		healthcheck.dbRecordsGeneratePassed += 1
	} else {
		healthcheck.dbRecordsGenerateFailed += 1
	}
	incrementDbRecordsGeneratedMutex.Unlock()
}

func IncrementDbRecordsProcessed(isPassed bool) {
	incrementDbRecordsProcessedMutex.Lock()
	if isPassed {
		healthcheck.dbRecordsProcessPassed += 1
	} else {
		healthcheck.dbRecordsProcessFailed += 1
	}
	incrementDbRecordsProcessedMutex.Unlock()
}

func IncrementESRecordsStored(isPassed bool) {
	incrementesRecordsStoredMutex.Lock()
	if isPassed {
		healthcheck.esRecordsStorePassed += 1
	} else {
		healthcheck.esRecordsStoreFailed += 1
	}
	incrementesRecordsStoredMutex.Unlock()
}

func resetHealthCheck() {
	healthcheck.dbRecordsGeneratePassed = 0
	healthcheck.dbRecordsGenerateFailed = 0
	healthcheck.dbRecordsProcessPassed = 0
	healthcheck.dbRecordsProcessFailed = 0
	healthcheck.esRecordsStorePassed = 0
	healthcheck.esRecordsStoreFailed = 0
}

func scheduleDisplayHealthCheck() {
	for {
		syncLastOperation()
		logger.WithField("dbRecordsGeneratePassed", healthcheck.dbRecordsGeneratePassed).WithField("dbRecordsGenerateFailed", healthcheck.dbRecordsGenerateFailed).WithField("dbRecordsProcessPassed", healthcheck.dbRecordsProcessPassed).WithField("dbRecordsProcessFailed", healthcheck.dbRecordsProcessFailed).WithField("esRecordsStorePassed", healthcheck.esRecordsStorePassed).WithField("esRecordsStoreFailed", healthcheck.esRecordsStoreFailed).WithField("pendingRecords", len(*dataChannelRef)).WithField("lastOperationRecord", lastOpTimestamp).Infof("Healthcheck for last 10 seconds")
		recoverPanic()
		resetHealthCheck()
		updateAndSaveConfig()
		time.Sleep(10 * time.Second)
	}
}

func syncLastOperation() {
	for {
		select {
		case lastOpTimestamp = <-*lastOpChannel:
		default:
			return
		}
	}
}

func updateAndSaveConfig() {
	var minTime uint32
	if lastOpTimestamp == nil {
		return
	}
	for _, v := range lastOpTimestamp {
		minTime = uint32(math.Max(float64(minTime), float64(v.T)))
	}
	(*config).Application.LastTimestampToResume = minTime
	Configuration.SaveApplicationConfig(*config)
}
