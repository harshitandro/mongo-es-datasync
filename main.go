package main

import (
	"github.com/harshitandro/mongo-es-datasync/src/ConfigurationStructs"
	"github.com/harshitandro/mongo-es-datasync/src/DataTransformationLayer"
	"github.com/harshitandro/mongo-es-datasync/src/ElasticDataLayer"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/harshitandro/mongo-es-datasync/src/MongoOplogs"
	"github.com/harshitandro/mongo-es-datasync/src/Utility/HealthCheck"
	"github.com/sirupsen/logrus"
	"os"
)

var logger *logrus.Entry

func init() {
	logger = Logging.GetLogger("Main", "Root")
}

func main() {
	//defer profile.Start(profile.MemProfile).Stop()
	var config ConfigurationStructs.ApplicationConfiguration
	config, err := ConfigurationStructs.LoadApplicationConfig()
	if err != nil {
		logger.Errorln("Error while loading application config. Exiting")
		os.Exit(1)
	}
	logger.Infoln("Application config loaded : ", config)

	bufferChannel := make(chan map[string]interface{})

	err = MongoOplogs.Initialise(config, &bufferChannel)
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
	HealthCheck.EnableHealthCheck(&bufferChannel, &MongoOplogs.LastOperation, &config)

	for {
		doc := <-bufferChannel
		//logger.Infoln("Oplog : ", doc)
		operation, collection, sender, oplogTimestamp, err := DataTransformationLayer.MongoOplogProcessor(&doc)
		if err != nil {
			logger.WithField("sender", sender).Warningln("Unable to transform Oplog Data, error : ", err)
			continue
		} else {
			for i := 0; i < 3; i++ {
				statusCode, isError := ElasticDataLayer.PushToElastic(doc, operation, collection)
				if isError {
					logger.WithField("oplogTimestamp", oplogTimestamp).WithField("sender", sender).WithField("operation", operation).WithField("collection", collection).
						WithField("docId", doc["mid"]).WithField("retry", i).Errorln("Failed to insert document into Elastic search. API Status : ", statusCode)
				} else {
					MongoOplogs.UpdateLastOperationDetails(sender, oplogTimestamp)
					break
				}
			}

		}

	}

}
