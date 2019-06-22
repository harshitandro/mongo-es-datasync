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
	configFile, _ := os.Open("app_conf.json")
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
	configFile, _ := os.Open("/tmp/app_conf.json")
	defer configFile.Close()
	encoder := json.NewEncoder(configFile)
	encoder.Encode(config)
}
