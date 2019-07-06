package main

import (
	"github.com/harshitandro/mongo-es-datasync/src/Configuration"
	"github.com/harshitandro/mongo-es-datasync/src/ElasticDataLayer"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/harshitandro/mongo-es-datasync/src/Models/ConfigurationModels"
	"github.com/harshitandro/mongo-es-datasync/src/Models/DatabaseModels/CommonDatabaseModels"
	"github.com/harshitandro/mongo-es-datasync/src/MongoOplogs"
	"github.com/harshitandro/mongo-es-datasync/src/Utility/HealthCheck"
	"github.com/sirupsen/logrus"
	"os"
)

var logger *logrus.Entry
var healthCheckChannel chan CommonDatabaseModels.LastOperation
var batchSize int

func init() {
	logger = Logging.GetLogger("Root")
	batchSize = 500
}

func main() {
	//defer profile.Start(profile.MemProfile).Stop()
	var config ConfigurationModels.ApplicationConfiguration
	config, err := Configuration.LoadApplicationConfig()
	if err != nil {
		logger.Errorln("Error while loading application config. Exiting")
		os.Exit(1)
	}
	logger.Infoln("Application config loaded : ", config)
	Logging.SetLogLevel(config.Application.LogLevel)

	bufferChannel := make(chan CommonDatabaseModels.OplogMessage, 5000)
	healthCheckChannel = make(chan CommonDatabaseModels.LastOperation, 10)

	if config.Elasticsearch.BatchProcessingSize > 0 {
		batchSize = config.Elasticsearch.BatchProcessingSize
	}

	err = MongoOplogs.Initialise(config, &bufferChannel, &healthCheckChannel)
	if err != nil {
		logger.Errorln("Error while creating Mongo Client : ", err)
		os.Exit(1)
	}

	err = ElasticDataLayer.Initialise(config)
	if err != nil {
		logger.Errorln("Error while creating Elasticsearch Client : ", err)
		os.Exit(1)
	}
	// Start healthcheck after everything
	HealthCheck.EnableHealthCheck(&bufferChannel, &healthCheckChannel, &config)

	var oplogNessageList []CommonDatabaseModels.OplogMessage
	var oplogNessage CommonDatabaseModels.OplogMessage
	var retryOplogList []CommonDatabaseModels.OplogMessage

	for {
		oplogNessageList = append(oplogNessageList, retryOplogList...)
		currBatchSize := batchSize - len(retryOplogList)
		retryOplogList = nil
		if currBatchSize > 0 {
			for i := 0; i < currBatchSize; i++ {
				oplogNessage = <-bufferChannel
				if oplogNessage.IsAnyNil() {
					logger.WithField("source", oplogNessage.Source).Warningln("Unable to transform Oplog Data, error : Incomplete oplog message : ", oplogNessageList)
					continue
				}
				oplogNessageList = append(oplogNessageList, oplogNessage)
			}
		}
		retryOplogList = ElasticDataLayer.PushToElastic(oplogNessageList)
		oplogNessageList = nil
	}

}
