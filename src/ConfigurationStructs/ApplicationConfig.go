package ConfigurationStructs

import (
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var logger *logrus.Entry

func init() {
	logger = Logging.GetLogger("ApplicationConfig", "Root")
}

type ApplicationConfiguration struct {
	Application struct {
		LastTimestampToResume uint32 `json:"LastTimestampToResume"`
	} `json:"Application"`
	Monogo struct {
		QueryRouterAddr string   `json:"QueryRouterAddr"`
		DbsToMonitor    []string `json:"DbsToMonitor"`
	} `json:"Monogo"`
	Elasticsearch struct {
		ElasticURL string `json:"ElasticUrl"`
	} `json:"Elasticsearch"`
}

func LoadApplicationConfig() (ApplicationConfiguration, error) {
	var config ApplicationConfiguration
	viper.SetConfigName("app_conf")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./src")
	viper.AddConfigPath("./src/conf")
	err := viper.ReadInConfig()
	if err != nil {
		logger.Errorln("Error reading application config : ", err)
		return config, err
	}

	err = viper.Unmarshal(&config)
	if err != nil {
		logger.Errorln("Error unmarshalling application config : ", err)
		return config, err
	}
	return config, nil
}
