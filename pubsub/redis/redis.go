package redis

import (
	"context"
	"strings"

	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/AnubhavUjjawal/MoMo/logger"
	"github.com/go-redis/redis"
)

type RedisPubSubClient struct {
	redisClient *redis.Client
}

var redisPubSubClient *RedisPubSubClient
var sugar = logger.GetSugaredLogger()

const taskStream = "TaskStream"
const taskCompleteStream = "TaskCompleteStream"
const workerGroup = "WorkerGroup"
const schedulerGroup = "schedulerGroup"

// GetClient returns a redis.Client
func (r *RedisPubSubClient) GetClient() *redis.Client {
	// TODO: Pickup, Addr, Password, DB, Username, PoolSize through env variables.
	// i.e, create a make client method
	if redisPubSubClient != nil {
		return redisPubSubClient.redisClient
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		PoolSize: 16,
	})
	redisPubSubClient = &RedisPubSubClient{redisClient}
	return redisPubSubClient.redisClient
}

func (r *RedisPubSubClient) PublishTaskInstanceToRun(cxt context.Context, ti *core.TaskInstance, dag *core.DAG) {
	args := redis.XAddArgs{Stream: taskStream, Values: map[string]interface{}{"TaskInstance": ti}}
	_, err := r.GetClient().XAdd(&args).Result()
	if err != nil {
		panic(err)
	}
	sugar.Infof("Pushed %s:%s to %s", ti.DagName, ti.TaskName, taskStream)
}

func (r *RedisPubSubClient) PublishTaskInstanceAsComplete(cxt context.Context, ti *core.TaskInstance, dag *core.DAG) {
	args := redis.XAddArgs{Stream: taskCompleteStream, Values: map[string]interface{}{"TaskInstance": ti}}
	_, err := r.GetClient().XAdd(&args).Result()
	if err != nil {
		panic(err)
	}
	sugar.Infof("Pushed %s:%s to %s", ti.DagName, ti.TaskName, taskStream)
}

// TODO: Ensure proper processing of messages and resend messages if they are
// kept for too long in XPENDING.
func (r *RedisPubSubClient) GetReadyToRunTasks(ctx context.Context, count int) ([]*core.TaskInstance, []interface{}) {
	args := redis.XReadGroupArgs{
		Streams: []string{taskStream, ">"},
		Block:   0, Count: int64(count),
		Group: workerGroup,
		// TODO: put unique Consumer name here.
		Consumer: "worker"}
	val, err := r.GetClient().XReadGroup(&args).Result()

	if err != nil {
		panic(err)
	}
	taskInstances := make([]*core.TaskInstance, 0)
	messageIds := make([]interface{}, 0)
	for _, marshaledTask := range val[0].Messages {
		taskInstance := &core.TaskInstance{}
		err = taskInstance.UnmarshalBinary([]byte(marshaledTask.Values["TaskInstance"].(string)))
		if err != nil {
			panic(err)
		}
		taskInstances = append(taskInstances, taskInstance)
		messageIds = append(messageIds, marshaledTask.ID)
	}

	return taskInstances, messageIds
}

// TODO: Ensure proper processing of messages and resend messages if they are
// kept for too long in XPENDING.
func (r *RedisPubSubClient) GetCompletedTasks(ctx context.Context, count int) ([]*core.TaskInstance, []interface{}) {
	args := redis.XReadGroupArgs{
		Streams: []string{taskCompleteStream, ">"},
		Block:   0, Count: int64(count),
		Group: schedulerGroup,
		// TODO: put unique Consumer name here.
		Consumer: "scheduler"}
	val, err := r.GetClient().XReadGroup(&args).Result()

	if err != nil {
		panic(err)
	}
	taskInstances := make([]*core.TaskInstance, 0)
	messageIds := make([]interface{}, 0)
	for _, marshaledTask := range val[0].Messages {
		taskInstance := &core.TaskInstance{}
		err = taskInstance.UnmarshalBinary([]byte(marshaledTask.Values["TaskInstance"].(string)))
		if err != nil {
			panic(err)
		}
		taskInstances = append(taskInstances, taskInstance)
		messageIds = append(messageIds, marshaledTask.ID)
	}

	return taskInstances, messageIds
}

func (r *RedisPubSubClient) AckTaskProcessed(ctx context.Context, id interface{}) {
	_, err := r.GetClient().XAck(taskStream, workerGroup, id.(string)).Result()
	if err != nil {
		panic(err)
	}
	sugar.Infow("Acked task message as processed.", "id", id.(string))
}

func (r *RedisPubSubClient) AckTaskCompletionProcessed(ctx context.Context, id interface{}) {
	_, err := r.GetClient().XAck(taskCompleteStream, schedulerGroup, id.(string)).Result()
	if err != nil {
		panic(err)
	}
	sugar.Infow("Acked task completion message as processed.", "id", id.(string))
}

func (r *RedisPubSubClient) CreateConsumerGroupTasks(ctx context.Context) {
	_, err := r.GetClient().XGroupCreateMkStream(taskStream, workerGroup, "0").Result()
	if err != nil {
		if !strings.HasPrefix(err.Error(), "BUSYGROUP") {
			panic(err)
		}
	}
}

func (r *RedisPubSubClient) CreateConsumerGroupTasksCompleted(ctx context.Context) {
	_, err := r.GetClient().XGroupCreateMkStream(taskCompleteStream, schedulerGroup, "0").Result()
	if err != nil {
		if !strings.HasPrefix(err.Error(), "BUSYGROUP") {
			panic(err)
		}
	}
}
