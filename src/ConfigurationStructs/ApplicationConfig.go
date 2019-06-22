package ConfigurationStructs

import (
	"bytes"
	"encoding/json"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var logger *logrus.Entry
var config *ApplicationConfiguration

func init() {
	logger = Logging.GetLogger("ApplicationConfig", "Root")
}

type ApplicationConfiguration struct {
	Application struct {
		LastTimestampToResume uint32 `json:"lasttimestamptoresume"`
	} `json:"application"`
	Monogo struct {
		QueryRouterAddr string   `json:"queryrouteraddr"`
		DbsToMonitor    []string `json:"dbstomonitor"`
	} `json:"monogo"`
	Elasticsearch struct {
		ElasticURL string `json:"elasticurl"`
	} `json:"elasticsearch"`
}

func LoadApplicationConfig() (ApplicationConfiguration, error) {
	viper.SetConfigName("app_conf")
	viper.AddConfigPath("/tmp")
	viper.AddConfigPath(".")
	viper.AddConfigPath("/")
	viper.AddConfigPath("/etc/mongo-es-sync")
	err := viper.ReadInConfig()
	if err != nil {
		logger.Errorln("Error reading application config : ", err)
		return *config, err
	}

	err = viper.Unmarshal(&config)
	if err != nil {
		logger.Errorln("Error unmarshalling application config : ", err)
		return *config, err
	}

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
	requestByte, err := json.Marshal(config)
	if err != nil {
		return
	}
	viper.MergeConfig(bytes.NewReader(requestByte))
	viper.WriteConfigAs("/tmp/app_conf.json")
}
