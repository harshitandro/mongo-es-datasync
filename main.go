package Mongo_ES_Data_Sync

import (
	"github.com/harshitandro/mongo-es-datasync/src/ConfigurationStructs"
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

	err = MongoOplogs.Initialise(config)
	if err != nil {
		logger.Errorln("Error while creating Mongo Client. Exiting")
		os.Exit(1)
	}

	bufferChannel := make(chan string)
	MongoOplogs.TailOplogs(&bufferChannel)
	for {
		logger.Infoln("Oplog : ", <-bufferChannel)
	}

}
