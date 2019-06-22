# Mongo-Elasticsearch Data Sync

A utility to tail a Sharded Replicated MongoDb cluster's oplog as datasource & populate given Elasticsearch cluster from it.

##### Phase : alpha

### Run

The application can be ran as a docker container using the following command.

` docker run -v /PATH/TO/CONFIG/app_conf.json:/app_conf.json  --name Mongo-ES-DataSync --image harshitandro/mongo-es-datasync:TAGNAME`

 - /PATH/TO/CONFIG/app_conf.json should be the path to application config file in prescribed format given below.
 - Docker image tag can be found at project's [dockerhub repo](https://hub.docker.com/r/harshitandro/mongo-es-datasync) .
 
### Configuration 
The config file has the following format: 
```
{
  "application": {
    "lastTimestampToResume": 0
  },
  "elasticsearch": {
    "elasticURL": "127.0.0.1:9200"
  },
  "db": {
    "mongo": {
      "dbsToMonitor": [
        "test"
      ],
      "queryRouterAddr": "127.0.0.1:27007",
      "auth": {
        "username" : "",
        "password": "",
        "source" : ""
      }
    }
  }
}
```

- `application.lastTimestampToResume` determines the mongoDB oplog timestamp from which the application should resume sync to ES.
- `elasticsearch.elasticURL` is the elastic URL of your ES cluster.
- `db.mongo.dbsToMonitor` is a list of dbs in your mongo which are to be synced to ES.
- `db.mongo.queryRouterAddr` is the address of your mongoDb's Query Router.
- [OPTIONAL]`db.mongo.auth` defines the authentication credentials for the given MongoDB.
    - `db.mongo.auth.username` defines the authentication username.
    - `db.mongo.auth.password` defines the authentication password.
    - `db.mongo.auth.source` defines the authentication database of the given username. 

### Features
Following feature set is currently present:
- [x] Oplog tailing from Sharded Replicated MongoDb cluster.
- [x] Auto resume from last operation state in case of application restart.
- [x] Auto reconnect in case of connectivity failure from either mongoDB or Elasticsearch.
- [x] Authentication for mongoDB connections.
- [ ] Authentication for elasticsearch connections.


### Compatibility
- Sharded Replicated MongoDb Cluster Version 3.6  & above
- Elasticsearch Version 6.2 & above

