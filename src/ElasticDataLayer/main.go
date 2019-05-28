package ElasticDataLayer

import (
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/sirupsen/logrus"
)

var logger *logrus.Entry

func init() {
	logger = Logging.GetLogger("ElasticDataLayer", "Root")
}
