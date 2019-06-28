package ElasticDataLayer

import (
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/harshitandro/mongo-es-datasync/src/Models/ConfigurationModels"
	"github.com/harshitandro/mongo-es-datasync/src/Models/DatabaseModels/CommonDatabaseModels"
	"github.com/harshitandro/mongo-es-datasync/src/Utility/HealthCheck"
	"github.com/sirupsen/logrus"
	"runtime/debug"
	"strings"
)

var logger *logrus.Entry
var esClient *elasticsearch.Client

func init() {
	logger = Logging.GetLogger("Root")
}

func recoverPanic(oplogMessage *CommonDatabaseModels.OplogMessage) {
	if r := recover(); r != nil {
		HealthCheck.IncrementESRecordsStored(false)
		logger.WithField("trace", string(debug.Stack())).Errorln("Panic while saving message to ES : ", r, "\n : ", *oplogMessage)
	}
}

// TODO: Add custom settings for any user provided index.
func Initialise(config ConfigurationModels.ApplicationConfiguration) error {
	var err error
	var (
		r map[string]interface{}
	)
	esConfig := elasticsearch.Config{
		Addresses: []string{"http://" + config.Elasticsearch.ElasticURL},
	}

	esClient, err = elasticsearch.NewClient(esConfig)
	// 1. Get cluster info
	//
	res, err := esClient.Info()

	if err != nil {
		logger.Fatalf("Error getting response: %s", err)
		return err
	}
	// Check response status
	if res.IsError() {
		logger.Fatalf("Error: %s", res.String())
		return err
	}
	// Deserialize the response into a map.
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		logger.Fatalf("Error parsing the response body: %s", err)
		return err
	}
	// Print client and server version numbers.
	logger.Printf("Elasticsearch Client version : %s", elasticsearch.Version)
	logger.Printf("Elasticsearch Server version : %s", r["version"].(map[string]interface{})["number"])
	logger.Println(strings.Repeat("~", 37))
	return nil
}

func PushToElastic(oplogMessage CommonDatabaseModels.OplogMessage) (int, bool) {
	defer recoverPanic(&oplogMessage)
	var res *esapi.Response
	var err error
	switch oplogMessage.Operation {
	case "i":
		fallthrough
	case "u":
		res, err = esClient.Index(
			strings.ToLower(oplogMessage.Collection),       // Index name
			esutil.NewJSONReader(oplogMessage.Data),        // Document body
			esClient.Index.WithDocumentID(oplogMessage.ID), // Document ID
			//esClient.Index.WithRefresh("true"),                 // Refresh
		)
	case "d":
		res, err = esClient.Delete(
			strings.ToLower(oplogMessage.Collection), // Index name
			oplogMessage.ID,                          // Document ID
			//esClient.Delete.WithRefresh("true"), // Refresh
		)
	default:
		logger.Errorf("Push to Elasticsearch failed due to unknown operation: %s\n", oplogMessage)
		HealthCheck.IncrementESRecordsStored(false)
		return 000, true
	}

	if err != nil {
		logger.Errorf("Push to Elasticsearch failed : %s\n", err)
		HealthCheck.IncrementESRecordsStored(false)
		return res.StatusCode, res.IsError()
	}
	if res == nil {
		logger.Errorf("Push to Elasticsearch failed : Response from ES is nil \n")
		HealthCheck.IncrementESRecordsStored(false)
		return 000, true
	}
	if res.IsError() {
		logger.Errorf("[%s] Error indexing document ID %s : %s", res.Status(), oplogMessage.ID, res.String())
		HealthCheck.IncrementESRecordsStored(false)
		return res.StatusCode, res.IsError()
	} else {
		defer res.Body.Close()
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			logger.Errorf("Error parsing the response body: %s", err)
			HealthCheck.IncrementESRecordsStored(false)
		} else {
			// Print the response status and indexed document version.
			logger.Debugln("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
			HealthCheck.IncrementESRecordsStored(true)
		}
		return res.StatusCode, res.IsError()
	}
}
