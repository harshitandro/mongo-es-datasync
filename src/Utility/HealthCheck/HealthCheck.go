package HealthCheck

import (
	"github.com/harshitandro/mongo-es-datasync/src/ConfigurationStructs"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"math"
	"sync"
	"time"
)

var logger *logrus.Entry
var healthcheck HealthCheck
var incrementDbRecordsGeneratedMutex sync.Mutex
var incrementDbRecordsProcessedMutex sync.Mutex
var incrementesRecordsStoredMutex sync.Mutex
var dataChannelRef *chan map[string]interface{}
var lastOpTimestamp *map[string]primitive.Timestamp
var config *ConfigurationStructs.ApplicationConfiguration

func init() {
	logger = Logging.GetLogger("HealthCheck", "Root")
}

func EnableHealthCheck(dataChannel *chan map[string]interface{}, lastOperation *map[string]primitive.Timestamp, applicationConfig *ConfigurationStructs.ApplicationConfiguration) {
	dataChannelRef = dataChannel
	lastOpTimestamp = lastOperation
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
		logger.WithField("dbRecordsGeneratePassed", healthcheck.dbRecordsGeneratePassed).WithField("dbRecordsGenerateFailed", healthcheck.dbRecordsGenerateFailed).WithField("dbRecordsProcessPassed", healthcheck.dbRecordsProcessPassed).WithField("dbRecordsProcessFailed", healthcheck.dbRecordsProcessFailed).WithField("esRecordsStorePassed", healthcheck.esRecordsStorePassed).WithField("esRecordsStoreFailed", healthcheck.esRecordsStoreFailed).WithField("pendingRecords", len(*dataChannelRef)).WithField("lastOperationRecord", *lastOpTimestamp).Infof("Healthcheck for last 10 seconds")
		resetHealthCheck()
		updateAndSaveConfig()
		time.Sleep(10 * time.Second)
	}
}

func updateAndSaveConfig() {
	var maxTime uint32
	for _, v := range *lastOpTimestamp {
		maxTime = uint32(math.Max(float64(maxTime), float64(v.T)))
	}
	(*config).Application.LastTimestampToResume = maxTime
	ConfigurationStructs.SaveApplicationConfig(*config)
}
