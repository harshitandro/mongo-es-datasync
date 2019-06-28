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

func init() {
	logger = Logging.GetLogger("Root")
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

	var oplogNessage CommonDatabaseModels.OplogMessage
	for {
		oplogNessage = <-bufferChannel
		if oplogNessage.IsAnyNil() {
			logger.WithField("source", oplogNessage.Source).Warningln("Unable to transform Oplog Data, error : Incomplete oplog message : ", oplogNessage)
			continue
		} else {
			for i := 0; i < 3; i++ {
				statusCode, isError := ElasticDataLayer.PushToElastic(oplogNessage)
				if isError {
					logger.WithField("oplogTimestamp", oplogNessage.Timestamp).WithField("source", oplogNessage.Source).WithField("operation", oplogNessage.Operation).WithField("collection", oplogNessage.Collection).
						WithField("docId", oplogNessage.ID).WithField("retry", i).Errorln("Failed to insert document into Elastic search. API Status : ", statusCode)
				} else {
					MongoOplogs.UpdateLastOperationDetails(oplogNessage.Source, oplogNessage.Timestamp)
					break
				}
			}

		}

	}

}
