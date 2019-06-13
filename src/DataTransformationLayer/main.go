package DataTransformationLayer

import (
	"fmt"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/harshitandro/mongo-es-datasync/src/MongoOplogs"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"strings"
)

var logger *logrus.Entry

func init() {
	logger = Logging.GetLogger("ElasticDataLayer", "Root")
}

func MongoOplogProcessor(doc *map[string]interface{}) (string, string, string, primitive.Timestamp, error) {
	var err error
	operationType := (*doc)["op"]
	sender := (*doc)["sender"].(string)
	ns := (*doc)["ns"]
	namespace := strings.Split(ns.(string), ".")
	timestamp := (*doc)["ts"].(primitive.Timestamp)
	if len(namespace) != 2 {
		return "", "", sender, timestamp, fmt.Errorf("Invalid operation. Unsupported namespace : %s ", namespace)
	}
	switch operationType {
	case "i", "d":

		(*doc)["o"].(map[string]interface{})["mid"] = (*doc)["o"].(map[string]interface{})["_id"].(primitive.ObjectID).Hex()
		objectID := (*doc)["o"].(map[string]interface{})["_id"].(primitive.ObjectID)
		delete((*doc)["o"].(map[string]interface{}), "_id")
		*doc = (*doc)["o"].(map[string]interface{})
		(*doc)["opTime"] = objectID.Timestamp()
		return operationType.(string), namespace[1], sender, timestamp, nil
	case "u":
		id := (*doc)["o2"].(map[string]interface{})["_id"].(primitive.ObjectID)

		*doc, err = MongoOplogs.GetRecordById(namespace[0], namespace[1], id.Hex())
		if (*doc) != nil {
			(*doc)["mid"] = id.Hex()
			delete(*doc, "_id")
		} else {
			return "", "", sender, timestamp, fmt.Errorf("No mongo record found for update operation by id: %s due to error : %s ", id.Hex(), err)
		}
		return operationType.(string), namespace[1], sender, timestamp, nil
	default:
		return "", "", sender, timestamp, fmt.Errorf("Unsupported operation : %s ", operationType)
	}
}
