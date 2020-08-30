package storage

import (
	"context"
	"time"

	"github.com/AnubhavUjjawal/MoMo/config"
	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/AnubhavUjjawal/MoMo/logger"
	"github.com/AnubhavUjjawal/MoMo/storage/redis"
)

// DataStore is the common interface which should be implemented to interact
// for persistence in different data stores, eg: redis, mysql.
// NOTE: They should be threadsafe to use.
// TODO: Add context to every method in this interface.
type DataStore interface {
	AddDag(cxt context.Context, dag *core.DAG)
	AddTask(cxt context.Context, task core.TaskInterface)
	AddDagRun(cxt context.Context, dag *core.DAG, schTime time.Time)
	AddOrUpdateTaskInstance(cxt context.Context, taskInstance *core.TaskInstance, dag *core.DAG)
	GetDag(cxt context.Context, dagName string) *core.DAG
	GetDagLastRun(cxt context.Context, dagName string) (schTime time.Time, complete bool)
	// GetTask(taskName string) *core.TaskInterface
	// GetAllTasks(dagName string) []*core.TaskInterface
	// GetTaskInstances(dagRunTime string)
	UpdateDagRunToComplete(cxt context.Context, dag *core.DAG, schTime time.Time)
	GetTaskInstances(cxt context.Context, dag *core.DAG, dagRunTime time.Time) map[string]*core.TaskInstance
	GetAllDags(cxt context.Context) chan *core.DAG
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
