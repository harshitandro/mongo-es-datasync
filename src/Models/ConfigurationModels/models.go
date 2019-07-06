package ConfigurationModels

type ApplicationConfiguration struct {
	Application   Application   `json:"application"`
	Elasticsearch Elasticsearch `json:"elasticsearch"`
	Db            Db            `json:"db"`
}
type Application struct {
	LastTimestampToResume uint32 `json:"lastTimestampToResume"`
	LogLevel              string `json:"logLevel"`
}
type Elasticsearch struct {
	ElasticURL          string `json:"elasticURL"`
	BatchProcessingSize int    `json:"batchProcessingSize"`
}
type Db struct {
	Mongo Mongo `json:"mongo"`
}
type Mongo struct {
	DbsToMonitor    []string  `json:"dbsToMonitor"`
	QueryRouterAddr string    `json:"queryRouterAddr"`
	Auth            MongoAuth `json:"auth"`
}
type MongoAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Source   string `json:"source"`
}

func (applicationConfiguration ApplicationConfiguration) IsNil() bool {
	if applicationConfiguration.Db.IsNil() && applicationConfiguration.Elasticsearch == (Elasticsearch{}) && applicationConfiguration.Application == (Application{}) {
		return true
	}
	return false
}

func (mongoAuth MongoAuth) IsNil() bool {
	if mongoAuth == (MongoAuth{}) {
		return true
	}
	return false
}

func (mongo Mongo) IsNil() bool {
	if mongo.Auth == (MongoAuth{}) && mongo.DbsToMonitor == nil && mongo.QueryRouterAddr == "" {
		return true
	}
	return false
}

func (db Db) IsNil() bool {
	if db.Mongo.IsNil() {
		return true
	}
	return false
}
