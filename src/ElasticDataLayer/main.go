package ElasticDataLayer

import (
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"github.com/harshitandro/mongo-es-datasync/src/ConfigurationStructs"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strings"
)

var logger *logrus.Entry
var esClient *elasticsearch.Client

func init() {
	logger = Logging.GetLogger("ElasticDataLayer", "Root")
}

func Initialise(config ConfigurationStructs.ApplicationConfiguration) error {
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

func PushToElastic(doc map[string]interface{}, operation string, collection string) error {
	var res *esapi.Response
	var err error

	objectId, err := primitive.ObjectIDFromHex(doc["mid"].(string))
	if err != nil {
		return fmt.Errorf("unable to fetch ObjectID for given doc : %s", doc)
	}

	switch operation {
	case "i":
		fallthrough
	case "u":
		res, err = esClient.Index(
			collection,                // Index name
			esutil.NewJSONReader(doc), // Document body
			esClient.Index.WithDocumentID(doc["mid"].(string)), // Document ID
			esClient.Index.WithRefresh("true"),                 // Refresh
		)
	case "d":
		res, err = esClient.Delete(
			collection,                          // Index name
			doc["mid"].(string),                 // Document ID
			esClient.Delete.WithRefresh("true"), // Refresh
		)

	}

	//defer res.Body.Close()
	if err != nil {
		logger.Errorf("Push to Elasticsearch failed : %s\n", err)
		return err
	}
	if res.IsError() {
		logger.Printf("[%s] Error indexing document ID %s", res.Status(), objectId.Hex())
		return nil
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			logger.Printf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and indexed document version.
			logger.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		}
	}
	return nil
}
