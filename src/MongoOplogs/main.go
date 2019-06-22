package MongoOplogs

import (
	"context"
	"errors"
	"fmt"
	"github.com/harshitandro/mongo-es-datasync/src/ConfigurationStructs"
	"github.com/harshitandro/mongo-es-datasync/src/Logging"
	"github.com/harshitandro/mongo-es-datasync/src/Utility/HealthCheck"
	"github.com/remeh/sizedwaitgroup"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"strings"
	"sync"
	"time"
)

type ControlBlock struct {
	shardAddr      string
	replicasetName string
	lastTimestamp  primitive.Timestamp
	call           uint8
}

var logger *logrus.Entry
var mongoClient *mongo.Client
var dbsToMonitor []string
var queryRouterAddr string
var timestampToResume primitive.Timestamp
var shardsAddr map[string]string
var dataOutputChannel *chan map[string]interface{}
var controlChannel chan ControlBlock
var shardCientWaitGroup sizedwaitgroup.SizedWaitGroup
var queryClientWaitGroup sizedwaitgroup.SizedWaitGroup
var queryClientMutex sync.Mutex
var LastOperation map[string]primitive.Timestamp
var authData options.Credential
var enableAuth bool

func init() {
	logger = Logging.GetLogger("MongoOplogs", "Root")
	shardsAddr = make(map[string]string)
}

func Initialise(config ConfigurationStructs.ApplicationConfiguration, outputChannel *chan map[string]interface{}) error {
	var err error
	queryClientWaitGroup = sizedwaitgroup.New(1)
	dataOutputChannel = outputChannel
	if dataOutputChannel == nil {
		return errors.New("given outputChannel is nil")
	}
	dbsToMonitor = config.Db.Mongo.DbsToMonitor
	queryRouterAddr = config.Db.Mongo.QueryRouterAddr
	timestampToResume = primitive.Timestamp{config.Application.LastTimestampToResume, 0}

	if (ConfigurationStructs.MongoAuth{}) != config.Db.Mongo.Auth {
		enableAuth = true
		authData.AuthSource = config.Db.Mongo.Auth.Source
		authData.Username = config.Db.Mongo.Auth.Username
		authData.Password = config.Db.Mongo.Auth.Password
		logger.Debugln("Enabling authentication based connection to MongoDB.")
	} else {
		logger.Debugln("Connecting without authentication to MongoDB")
	}

	queryClientWaitGroup.Add()
	err = createQueryClient(&queryClientWaitGroup)
	if err != nil {
		return err
	}
	isSuccessful := detectMongoConfig()
	if !isSuccessful {
		return errors.New("Unable to find supported mongo cluster config in the given cluster")
	}
	logger.Infoln("Mongo Client created successfully to Mongo Cluster : ", queryRouterAddr)
	logger.Infoln("Monitoring DBs: ", dbsToMonitor)
	logger.Infoln("Starting tailing oplogs for monitored dbs since timestamp : ", timestampToResume)
	go dispatcher()

	return nil
}

//TODO : Need to rewrite the detection for Replicas omly as Shards don't have the oplogs.
func detectMongoConfig() (isSuccessful bool) {
	isSuccessful = false
	adminDatabase := mongoClient.Database("admin")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	result := adminDatabase.RunCommand(ctx, bson.D{{"listShards", true}})
	if result.Err() != nil {
		logger.Warningln("Error running command to get shard config : ", result.Err())
	}

	shardConfig, err := result.DecodeBytes()
	if err != nil {
		logger.Warningln("Error decoding shard config : ", err)
	}

	shardResult := gjson.Get(shardConfig.String(), "shards.#.host")

	if !shardResult.Exists() {
		logger.Warningln("Unable to detect sharded config in current mongo cluster")
	} else {
		for _, shard := range shardResult.Array() {
			shardInfo := strings.Split(shard.String(), "/")
			shardsAddr[shardInfo[0]] = shardInfo[1]
		}
	}

	if len(shardsAddr) == 0 {
		logger.Warningln("Replica list empty. Unable to detect replica config in current mongo cluster")
	} else {
		logger.Debugln("Creating control channel with buffer size : ", len(shardsAddr))
		controlChannel = make(chan ControlBlock, len(shardsAddr)+1)
		shardCientWaitGroup = sizedwaitgroup.New(len(shardsAddr))
		LastOperation = make(map[string]primitive.Timestamp)

		for id, addr := range shardsAddr {
			logger.Infoln("Replica found: ID ", id, " : ", addr)
			LastOperation[addr] = timestampToResume
		}
		logger.Infoln("Total replicas found: ", len(shardsAddr))
		isSuccessful = true
	}
	return
}

func createQueryClient(waitGroup *sizedwaitgroup.SizedWaitGroup) error {
	queryClientMutex.Lock()
	defer (*waitGroup).Done()
	defer queryClientMutex.Unlock()
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if mongoClient != nil {
		// Check the connection
		err = mongoClient.Ping(ctx, nil)

		if err != nil {
			logger.Errorln("Error while pinging Mongo Cluster: ", err)
		} else {
			logger.Infoln("Query client already connected & is healthy")
			return nil
		}

	}
	if enableAuth {
		mongoClient, err = mongo.NewClient(options.Client().SetDirect(true).ApplyURI("mongodb://" + queryRouterAddr).SetAuth(authData).SetSocketTimeout(15 * time.Second).SetConnectTimeout(15 * time.Second))
	} else {
		mongoClient, err = mongo.NewClient(options.Client().SetDirect(true).ApplyURI("mongodb://" + queryRouterAddr).SetSocketTimeout(15 * time.Second).SetConnectTimeout(15 * time.Second))
	}
	if err != nil {
		logger.Errorln("Error while creating Mongo Query Client : ", err)
		return err
	}
	err = mongoClient.Connect(ctx)
	if err != nil {
		logger.Errorln("Error while connecting to Mongo Query Router : ", err)
		return err
	}
	// Check the connection
	err = mongoClient.Ping(context.Background(), nil)

	if err != nil {
		logger.Errorln("Error while pinging Mongo Cluster: ", err)
		return err
	}
	return nil
}

func GetRecordById(db string, collection string, objectIDHex string) (map[string]interface{}, error) {

	collectionObj := mongoClient.Database(db).Collection(collection)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	objectID, err := primitive.ObjectIDFromHex(objectIDHex)
	if err != nil {

		return nil, err
	}

	// TODO: Need better approach to distinguish between no record being present or connection failures.
	result := collectionObj.FindOne(ctx, bson.D{{"_id", objectID}})
	if result.Err() != nil {
		controlChannel <- ControlBlock{call: 0}
		return nil, fmt.Errorf("can't get mongo record by ObjectID : %s", result.Err())
	}
	var doc map[string]interface{}
	result.Decode(&doc)
	return doc, nil
}

func createShardClient(replicasetName string, shardAddr string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	var client *mongo.Client
	var err error
	if enableAuth {
		client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+shardAddr+"/").SetAuth(authData).SetReadPreference(readpref.SecondaryPreferred()).SetReplicaSet(replicasetName).SetSocketTimeout(15*time.Second).SetConnectTimeout(15*time.Second))
	} else {
		client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://"+shardAddr+"/").SetReadPreference(readpref.SecondaryPreferred()).SetReplicaSet(replicasetName).SetSocketTimeout(15*time.Second).SetConnectTimeout(15*time.Second))
	}
	defer cancel()

	if err != nil {
		logger.WithField("shardAddr", shardAddr).Errorln("Error while creating Mongo Shard Client : ", err)
		return nil, err
	}

	// Check the connection
	err = mongoClient.Ping(ctx, nil)

	if err != nil {
		logger.WithField("shardAddr", shardAddr).Errorln("Error while pinging Mongo Shard : ", err)
		return nil, err
	}
	return client, nil
}

func closeShardConnection(shardAddr string, replicasetName string, client *mongo.Client, ctx context.Context, waitGroup *sizedwaitgroup.SizedWaitGroup) {
	defer (*waitGroup).Done()
	logger.WithField("mongoShard", shardAddr).Warningln("Closing connection to the shard")
	if client != nil {
		c, _ := context.WithTimeout(ctx, 5*time.Second)
		client.Disconnect(c)
	}
	controlChannel <- ControlBlock{shardAddr: shardAddr, replicasetName: replicasetName, lastTimestamp: LastOperation[shardAddr], call: 1}
}

func tailOplogForShard(shardAddr string, replicasetName string, timestampToResumeTail primitive.Timestamp, waitGroup *sizedwaitgroup.SizedWaitGroup) {
	ctx := context.Background()
	client, err := createShardClient(replicasetName, shardAddr)
	defer closeShardConnection(shardAddr, replicasetName, client, ctx, waitGroup)

	if err != nil {
		logger.Errorln("Unable to create connection to Shard : ", shardAddr, " : ", err)
		return
	}
	logger.WithField("mongoShard", shardAddr).Infoln("Started tailing oplogs from timestamp :", timestampToResumeTail)
	database := client.Database("local").Collection("oplog.rs")
	opts := options.FindOptions{}

	cursor, err := database.Find(ctx, bson.D{{"ts", bson.D{{"$gte", timestampToResumeTail}}}, {"fromMigrate", bson.D{{"$exists", false}}}, {"ns", bson.D{{"$regex", "^" + "(" + strings.Join(dbsToMonitor, "|") + ")" + "\\.([a-zA-Z0-9]+)"}}}}, opts.SetCursorType(options.TailableAwait))
	if err != nil {
		logger.WithField("mongoShard", shardAddr).Errorln("Error while tailing oplog : ", err)
		return
	}

	for cursor.Next(ctx) {
		var m map[string]interface{}
		err = cursor.Decode(&m)
		if m == nil {
			logger.WithField("mongoShard", shardAddr).Warningln("Empty Cursor data :", cursor.Err())
			HealthCheck.IncrementDbRecordsGenerated(false)
			break
		} else if err == nil {
			//logger.WithField("mongoShard", shardAddr).Warningln("Feeding Cursor data :", m)
			m["sender"] = shardAddr
			*dataOutputChannel <- m
			HealthCheck.IncrementDbRecordsGenerated(true)
		} else {
			logger.WithField("mongoShard", shardAddr).Errorln("Error while unmarshling bson raw to map : ", cursor.Current.String())
			HealthCheck.IncrementDbRecordsGenerated(false)
		}
	}
	if cursor.Err() != nil {
		logger.WithField("mongoShard", shardAddr).Warningln("Tail oplog exiting due to error :", cursor.Err())
	} else {
		logger.WithField("mongoShard", shardAddr).Warningln("Tail oplog exiting")
	}
}

func dispatcher() {
	for replicasetName, shardAddr := range shardsAddr {
		controlChannel <- ControlBlock{shardAddr: shardAddr, replicasetName: replicasetName, lastTimestamp: timestampToResume, call: 1}
	}
	var command ControlBlock
	for {
		command = <-controlChannel
		logger.Infoln("Received control command to start oplog tailing for : "+string(len(controlChannel))+": ", command)
		switch command.call {
		case 0:
			queryClientWaitGroup.Add()
			go createQueryClient(&queryClientWaitGroup)
		case 1:
			shardCientWaitGroup.Add()
			go tailOplogForShard(command.shardAddr, command.replicasetName, command.lastTimestamp, &shardCientWaitGroup)
		}

	}
}

func UpdateLastOperationDetails(sender string, lastTimestamp primitive.Timestamp) {
	LastOperation[sender] = lastTimestamp
}
