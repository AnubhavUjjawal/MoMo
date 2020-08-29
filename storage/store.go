package storage

import (
	"time"

	"github.com/AnubhavUjjawal/MoMo/config"
	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/AnubhavUjjawal/MoMo/logger"
	"github.com/AnubhavUjjawal/MoMo/storage/redis"
)

// DataStore is the common interface which should be implemented to interact
// for persistence in different data stores, eg: redis, mysql.
// NOTE: They should be threadsafe to use.
type DataStore interface {
	AddDag(dag *core.DAG)
	AddTask(task core.TaskInterface)
	AddDagRun(dag *core.DAG, schTime time.Time)
	AddTaskInstance(taskInstance *core.TaskInstance, dag *core.DAG)
	GetDag(dagName string) *core.DAG
	GetDagLastRun(dagName string) (schTime time.Time, complete bool)
	// GetTask(taskName string) *core.TaskInterface
	// GetAllTasks(dagName string) []*core.TaskInterface
	// GetTaskInstances(dagRunTime string)
	GetTaskInstances(dagName string, dagRunTime time.Time) map[string]*core.TaskInstance
	GetAllDags() chan *core.DAG
}

func DataStoreClient() DataStore {
	sugar := logger.GetSugaredLogger()
	switch config.GetDataStore() {
	case "REDIS":
		return &redis.RedisDataStoreClient{}
	default:
		sugar.Panicf("can't find DataStoreClient %s", config.GetDataStore())
	}
	return nil
}
