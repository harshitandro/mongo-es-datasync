package CommonDatabaseModels

import "go.mongodb.org/mongo-driver/bson/primitive"

type ControlBlock struct {
	ShardAddr      string
	ReplicasetName string
	LastTimestamp  primitive.Timestamp
	Call           uint8
}

type OplogMessage struct {
	ID         string
	Operation  string
	Collection string
	Source     string
	Timestamp  primitive.Timestamp
	Data       map[string]interface{}
	Retry      int
}

type LastOperation map[string]primitive.Timestamp

func (oplogMessage OplogMessage) IsAnyNil() bool {
	if oplogMessage.ID == "" || oplogMessage.Operation == "" || oplogMessage.Collection == "" || oplogMessage.Source == "" || oplogMessage.Data == nil {
		return true
	}
	return false
}
