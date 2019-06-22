package ConfigurationStructs

import (
	"encoding/json"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/sirupsen/logrus"
	"os"
)

var logger *logrus.Entry
var config *ApplicationConfiguration

func init() {
	logger = Logging.GetLogger("ApplicationConfig", "Root")
}

type ApplicationConfiguration struct {
	Application   Application   `json:"application"`
	Elasticsearch Elasticsearch `json:"elasticsearch"`
	Db            Db            `json:"db"`
}
type Application struct {
	LastTimestampToResume uint32 `json:"lastTimestampToResume"`
}
type Elasticsearch struct {
	ElasticURL string `json:"elasticURL"`
}
type Db struct {
	Mongo Mongo `json:"mongo"`
}
type Mongo struct {
	DbsToMonitor    []string  `json:"dbsToMonitor"`
	QueryRouterAddr string    `json:"queryRouterAddr"`
	Auth            MongoAuth `json:"auth"`
}
type MongoAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Source   string `json:"source"`
}

func LoadApplicationConfig() (ApplicationConfiguration, error) {
	var configFile *os.File
	var err error
	configFile, err = os.Open("/tmp/app_conf.json")
	if err != nil {
		configFile, err = os.Open("app_conf.json")
		if err != nil {
			configFile, err = os.Open("/app_conf.json")
			if err != nil {
				configFile, err = os.Open("/etc/mongo-es-sync/app_conf.json")
				if err != nil {
					return ApplicationConfiguration{}, err
				}
			}
		}
	}
	defer configFile.Close()
	decoder := json.NewDecoder(configFile)
	decoder.Decode(&config)
	return *config, nil
}

func GetApplicationConfig() ApplicationConfiguration {
	if config == nil {
		_, err := LoadApplicationConfig()
		if err != nil {
			return ApplicationConfiguration{}
		}
	}
	return *config
}

func SaveApplicationConfig(config ApplicationConfiguration) {
	configFile, err := os.OpenFile("/tmp/app_conf.json", os.O_WRONLY|os.O_CREATE, 0755)
	logger.Errorln(err)
	defer configFile.Close()
	encoder := json.NewEncoder(configFile)
	logger.Errorln(encoder.Encode(config))
}
