package ElasticDataLayer

import (
	"context"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/harshitandro/mongo-es-datasync/src/Models/ConfigurationModels"
	"github.com/harshitandro/mongo-es-datasync/src/Models/DatabaseModels/CommonDatabaseModels"
	"github.com/harshitandro/mongo-es-datasync/src/Utility/HealthCheck"
	"github.com/olivere/elastic"
	esConfig "github.com/olivere/elastic/config"
	"github.com/sirupsen/logrus"
	"runtime/debug"
	"strings"
)

var logger *logrus.Entry
var esClient *elastic.Client

func init() {
	logger = Logging.GetLogger("Root")

}

func recoverPanic(oplogMessage *[]CommonDatabaseModels.OplogMessage) {
	if r := recover(); r != nil {
		HealthCheck.IncrementESRecordsStored(false)
		logger.WithField("trace", string(debug.Stack())).Errorln("Panic while saving message to ES : ", r, "\n : ", *oplogMessage)
	}
}

// TODO: Add custom settings for any user provided index.
func Initialise(config ConfigurationModels.ApplicationConfiguration) error {
	var (
		err error
	)
	ctx := context.Background()

	elasticURL := config.Elasticsearch.ElasticURL
	elasticConfig := esConfig.Config{
		URL: elasticURL,
	}

	esClient, err = elastic.NewClientFromConfig(&elasticConfig)
	if err != nil {
		logger.Fatalf("Error loading Elastic Config : %s : %s", elasticConfig, err)
	}

	// Ping the Elasticsearch server to get e.g. the version number
	info, code, err := esClient.Ping(elasticURL).Do(ctx)
	if err != nil {
		logger.Fatalf("Error pinging Elastic Cluster at %s. Response : %s : %s : %s", elasticURL, code, err)
	}

	logger.Infof("Elasticsearch Cluster info : %s", info)

	return nil
}

func PushToElastic(oplogMessage []CommonDatabaseModels.OplogMessage) []CommonDatabaseModels.OplogMessage {
	defer recoverPanic(&oplogMessage)
	var err error

	requests := make([]elastic.BulkableRequest, 0)
	failedOplogs := make([]CommonDatabaseModels.OplogMessage, 0)
	bulkRequest := esClient.Bulk()
	logger.Debugln("Batch received of length : ", len(oplogMessage))
	for _, op := range oplogMessage {
		var req elastic.BulkableRequest
		switch op.Operation {
		case "i":
			fallthrough
		case "u":
			req = elastic.NewBulkIndexRequest().Index(strings.ToLower(op.Collection)).Id(op.ID).Doc(op.Data)
			requests = append(requests, req)
		case "d":
			req = elastic.NewBulkDeleteRequest().Index(strings.ToLower(op.Collection)).Id(op.ID)
			requests = append(requests, req)
		default:
			logger.Errorf("Push to Elasticsearch failed due to unknown operation: %s\n", oplogMessage)
			HealthCheck.IncrementESRecordsStored(false)
		}
		if req != nil {
			bulkRequest.Add(req)
		}
	}

	totalRequests := bulkRequest.NumberOfActions()

	bulkResponse, err := bulkRequest.Do(context.Background())

	if err != nil {
		logger.Errorf("Push to Elasticsearch failed due to error : %s", err)
		failedOplogs = oplogMessage
		HealthCheck.IncrementESRecordsStoredByCount(false, int32(len(failedOplogs)))
		return failedOplogs
	}

	if bulkResponse == nil {
		logger.Errorf("Push to Elasticsearch failed. Response from ES is nil \n")
		failedOplogs = oplogMessage
		HealthCheck.IncrementESRecordsStoredByCount(false, int32(len(failedOplogs)))
		return failedOplogs
	}

	if len(bulkResponse.Failed()) != 0 {
		failedID := make([]string, 0)
		for _, item := range bulkResponse.Failed() {
			failedID = append(failedID, item.Id)
			for _, message := range oplogMessage {
				if message.ID == item.Id && message.Retry <= 3 {
					message.Retry += 1
					failedOplogs = append(failedOplogs, message)
				} else {
					logger.Errorf("Discarding to reattempt storing document IDs %s due to max retry %s", item.Id, message.Retry)
				}
			}
		}

		HealthCheck.IncrementESRecordsStoredByCount(false, int32(len(bulkResponse.Failed())))
		HealthCheck.IncrementESRecordsStoredByCount(true, int32(len(bulkResponse.Succeeded())))
		logger.Errorf("Failed to store %s documents out of %s. Failed IDs to be rescheduled are : %s", len(bulkResponse.Failed()), totalRequests, failedID)
		return failedOplogs
	} else {
		// Print the response status and indexed document version.
		logger.WithField("totalDoc", totalRequests).WithField("passedDoc", len(bulkResponse.Succeeded())).WithField("failedDoc", len(bulkResponse.Failed())).WithField("inserts", len(bulkResponse.Indexed())).WithField("deletes", len(bulkResponse.Deleted())).WithField("updates", len(bulkResponse.Updated())).Debugln("Batch stored to Elasticsearch")
		HealthCheck.IncrementESRecordsStoredByCount(true, int32(len(bulkResponse.Succeeded())))
		return failedOplogs
	}
}
