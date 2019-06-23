package Configuration

import (
	"encoding/json"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/harshitandro/mongo-es-datasync/src/Models/ConfigurationModels"
	"github.com/sirupsen/logrus"
	"os"
)

var logger *logrus.Entry
var config ConfigurationModels.ApplicationConfiguration

func init() {
	logger = Logging.GetLogger("Root")
}

func LoadApplicationConfig() (ConfigurationModels.ApplicationConfiguration, error) {
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
					return ConfigurationModels.ApplicationConfiguration{}, err
				}
			}
		}
	}
	defer configFile.Close()
	decoder := json.NewDecoder(configFile)
	decoder.Decode(&config)
	return config, nil
}

func GetApplicationConfig() ConfigurationModels.ApplicationConfiguration {
	if config.IsNil() {
		_, err := LoadApplicationConfig()
		if err != nil {
			return ConfigurationModels.ApplicationConfiguration{}
		}
	}
	return config
}

func SaveApplicationConfig(config ConfigurationModels.ApplicationConfiguration) {
	configFile, _ := os.OpenFile("/tmp/app_conf.json", os.O_WRONLY|os.O_CREATE, 0755)
	defer configFile.Close()
	encoder := json.NewEncoder(configFile)
	encoder.Encode(config)
}
