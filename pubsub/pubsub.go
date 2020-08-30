package pubsub

import (
	"context"

	"github.com/AnubhavUjjawal/MoMo/config"
	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/AnubhavUjjawal/MoMo/logger"
	"github.com/AnubhavUjjawal/MoMo/pubsub/redis"
)

// PubSub is the common interface which should be implemented to interact
// for pub sub in different data stores, eg: redis, kafka.
// NOTE: They should be threadsafe to use.
type PubSub interface {
	PublishTaskInstanceToRun(cxt context.Context, ti *core.TaskInstance, dag *core.DAG)
	GetReadyToRunTasks(ctx context.Context, num int) ([]*core.TaskInstance, []interface{})
	GetCompletedTasks(ctx context.Context, count int) ([]*core.TaskInstance, []interface{})
	CreateConsumerGroupTasks(ctx context.Context)
	CreateConsumerGroupTasksCompleted(ctx context.Context)
	// AckTaskProcessed & AckTaskCompletionProcessed might be used by some
	// brokers, might not be used by some.
	AckTaskProcessed(ctx context.Context, taskID interface{})
	AckTaskCompletionProcessed(ctx context.Context, id interface{})
	PublishTaskInstanceAsComplete(cxt context.Context, ti *core.TaskInstance, dag *core.DAG)
}

func PubSubClient() PubSub {
	sugar := logger.GetSugaredLogger()
	switch config.GetPubSubClient() {
	case "REDIS":
		return &redis.RedisPubSubClient{}
	default:
		sugar.Panicf("can't find PubSubClient %s", config.GetPubSubClient())
	}
	return nil
}
