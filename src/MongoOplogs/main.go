package MongoOplogs

import (
	"context"
	"errors"
	"github.com/harshitandro/mongo-es-datasync/src/ConfigurationStructs"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"strings"
	"time"
)

var logger *logrus.Entry
var mongoClient *mongo.Client
var dbsToMonitor []string
var queryRouterAddr string
var timestampToResume uint32
var shardsAddr map[string]string

func init() {
	logger = Logging.GetLogger("MongoOplogs", "Root")
	shardsAddr = make(map[string]string)
}

func detectMongoConfig() error {

	adminDatabase := mongoClient.Database("admin")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result := adminDatabase.RunCommand(ctx, bson.D{{"listShards", true}})
	if result.Err() != nil {
		return result.Err()
	}
	shardConfig, err := result.DecodeBytes()
	if err != nil {
		return err
	}

	shardResult := gjson.Get(shardConfig.String(), "shards.#.host")

	if !shardResult.Exists() {
		return errors.New("Unable to detect sharded config in current mongo cluster.")
	}

	for _, shard := range shardResult.Array() {
		shardInfo := strings.Split(shard.String(), "/")
		shardsAddr[shardInfo[0]] = shardInfo[1]

	}

	if len(shardsAddr) == 0 {
		return errors.New("Shard address list empty. Unable to detect sharded config in current mongo cluster.")
	}

	return nil
}

func Initialise(config ConfigurationStructs.ApplicationConfiguration) error {
	var err error
	dbsToMonitor = config.Monogo.DbsToMonitor
	queryRouterAddr = config.Monogo.QueryRouterAddr
	timestampToResume = config.Application.LastTimestampToResume
	mongoClient, err = mongo.NewClient(options.Client().ApplyURI("mongodb://" + queryRouterAddr))
	if err != nil {
		logger.Errorln("Error while creating Mongo Client : ", err)
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = mongoClient.Connect(ctx)
	if err != nil {
		logger.Errorln("Error while connecting to Mongo Query Router : ", err)
		return err
	}

	// Check the connection
	err = mongoClient.Ping(ctx, nil)

	if err != nil {
		logger.Fatalln("Error while pinging Mongo Cluster: ", err)
		return err
	}
	err = detectMongoConfig()
	if err != nil {
		logger.Fatalln("Error while detecting Mongo Cluster Sharding info: ", err)
		return err
	}
	logger.Infoln("Mongo Client created successfully to Mongo Cluster : ", queryRouterAddr)
	logger.Infoln("Monitoring DBs: ", dbsToMonitor)
	logger.Infoln("Starting tailing oplogs for monitored dbs since timestamp : ", timestampToResume)
	logger.Info("Total Shards found: ", len(shardsAddr))
	for id, addr := range shardsAddr {
		logger.Info("Shard found: ID ", id, " : ", addr)
	}
	return nil
}

func createShardClient(replicasetName string, shardAddr string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+shardAddr+"/").SetReadPreference(readpref.SecondaryPreferred()).SetReplicaSet(replicasetName))
	defer cancel()
	if err != nil {
		logger.Errorln("Error while creating Mongo Shard Client : ", shardAddr, " : ", err)
		client.Disconnect(ctx)
		return nil, err
	}

	// Check the connection
	err = mongoClient.Ping(ctx, nil)

	if err != nil {
		logger.Errorln("Error while pinging Mongo Shard : ", shardAddr, " : ", err)
		client.Disconnect(ctx)
		return nil, err
	}
	return client, nil
}

func tailOplogForShard(shardAddr string, client *mongo.Client, bufferChannel *chan map[string]interface{}) error {
	ctx := context.Background()
	defer client.Disconnect(ctx)

	logger.WithField("mongoShard", shardAddr).Debugln("Started tailing oplogs")
	database := client.Database("local").Collection("oplog.rs")
	opts := options.FindOptions{}
	cursor, err := database.Find(ctx, bson.D{{"ts", bson.D{{"$gt", bsonx.Timestamp(timestampToResume, 0)}}}, {"fromMigrate", bson.D{{"$exists", false}}}, {"ns", bson.D{{"$regex", "^" + "(" + strings.Join(dbsToMonitor, "|") + ")" + "\\.([a-zA-Z0-9]+)"}}}}, opts.SetCursorType(options.TailableAwait))
	defer cursor.Close(ctx)
	if err != nil {
		logger.WithField("mongoShard", shardAddr).Errorln("Error while tailing oplog : ", err)
		return err
	}

	for cursor.Next(ctx) {
		var m map[string]interface{}
		err = cursor.Decode(&m)
		if err == nil {
			*bufferChannel <- m
		} else {
			logger.WithField("mongoShard", shardAddr).Errorln("Error while unmarshling bson raw to map : ", cursor.Current.String())
		}
	}
	return nil
}

func TailOplogs(bufferChannel *chan map[string]interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for replicasetName, shardAddr := range shardsAddr {
		client, err := createShardClient(replicasetName, shardAddr)
		if err != nil {
			logger.Errorln("Unable to create connection to Shard : ", shardAddr, " : ", err)
			client.Disconnect(ctx)
			return err
		}
		go tailOplogForShard(shardAddr, client, bufferChannel)
	}
	return nil
}

func GetRecordById(db string, collection string, objectIDHex string) map[string]interface{} {
	collectionObj := mongoClient.Database(db).Collection(collection)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	objectID, err := primitive.ObjectIDFromHex(objectIDHex)
	if err != nil {
		return nil
	}
	result := collectionObj.FindOne(ctx, bson.D{{"_id", objectID}})
	var doc map[string]interface{}
	result.Decode(&doc)
	return doc
}
