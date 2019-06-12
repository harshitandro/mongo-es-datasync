package main

import (
	"github.com/harshitandro/mongo-es-datasync/src/ConfigurationStructs"
	"github.com/harshitandro/mongo-es-datasync/src/DataTransformationLayer"
	"github.com/harshitandro/mongo-es-datasync/src/ElasticDataLayer"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/harshitandro/mongo-es-datasync/src/MongoOplogs"
	"github.com/sirupsen/logrus"
	"os"
)

var logger *logrus.Entry

func init() {
	logger = Logging.GetLogger("Main", "Root")
}

func main() {
	//defer profile.Start(profile.MemProfile).Stop()
	config, err := ConfigurationStructs.LoadApplicationConfig()
	if err != nil {
		logger.Errorln("Error while loading application config. Exiting")
		os.Exit(1)
	}
	logger.Debugln("Application config loaded : ", config)

	bufferChannel := make(chan map[string]interface{})

	err = MongoOplogs.Initialise(config, &bufferChannel)
	if err != nil {
		logger.Errorln("Error while creating Mongo Client. Exiting")
		os.Exit(1)
	}

	err = ElasticDataLayer.Initialise(config)
	if err != nil {
		logger.Errorln("Error while creating Elasticsearch Client. Exiting")
		os.Exit(1)
	}

	for {
		doc := <-bufferChannel
		//logger.Infoln("Oplog : ", doc)
		operation, collection, err := DataTransformationLayer.OplogProcessor(&doc)
		if err != nil {
			logger.Warningln("Unable to transform Oplog Data, error : ", err)
			continue
		} else {
			logger.WithField("operation", operation).WithField("collection", collection).Infoln("Document Length : ", len(doc))
		}
		//ElasticDataLayer.PushToElastic(doc, operation, collection)
	}

}
