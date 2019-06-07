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

func OplogProcessor(doc *map[string]interface{}) (string, string, error) {
	operationType := (*doc)["op"]
	switch operationType {
	case "i", "d":
		ns := (*doc)["ns"]
		namespace := strings.Split(ns.(string), ".")
		if len(namespace) != 2 {
			return "", "", fmt.Errorf("Invalid insert/delete operation. Unsupported namespace : %s ", namespace)
		}
		(*doc)["o"].(map[string]interface{})["mid"] = (*doc)["o"].(map[string]interface{})["_id"].(primitive.ObjectID).Hex()
		objectID := (*doc)["o"].(map[string]interface{})["_id"].(primitive.ObjectID)
		delete((*doc)["o"].(map[string]interface{}), "_id")
		*doc = (*doc)["o"].(map[string]interface{})
		(*doc)["opTime"] = objectID.Timestamp()
		return operationType.(string), namespace[1], nil
	case "u":
		ns := (*doc)["ns"]
		namespace := strings.Split(ns.(string), ".")
		id := (*doc)["o2"].(map[string]interface{})["_id"].(primitive.ObjectID)
		if len(namespace) != 2 {
			return "", "", fmt.Errorf("Invalid update operation. Unsupported namespace : %s ", namespace)
		}
		*doc = MongoOplogs.GetRecordById(namespace[0], namespace[1], id.Hex())
		if (*doc) != nil {
			(*doc)["mid"] = id.Hex()
			delete(*doc, "_id")
		} else {
			return "", "", fmt.Errorf("No record found for update operation by id: %s ", id.Hex())
		}
		return operationType.(string), namespace[1], nil
	default:
		return "", "", fmt.Errorf("Unsupported operation : %s ", operationType)
	}
}
