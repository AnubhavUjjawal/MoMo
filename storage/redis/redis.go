package redis

// package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AnubhavUjjawal/MoMo/logger"

	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/go-redis/redis/v8"
)

type RedisDataStoreClient struct {
	redisClient *redis.Client
}

var redisDataStoreClient *RedisDataStoreClient
var sugar = logger.GetSugaredLogger()

// GetClient returns a redis.Client
func (r *RedisDataStoreClient) GetClient() *redis.Client {
	// TODO: Pickup, Addr, Password, DB, Username, PoolSize through env variables.
	// i.e, create a make client method
	if redisDataStoreClient != nil {
		return redisDataStoreClient.redisClient
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		PoolSize: 16,
	})
	redisDataStoreClient = &RedisDataStoreClient{redisClient}
	return redisDataStoreClient.redisClient
}

//TODO: use ctx passed in methods which interact with GetClient
//TODO: Use Redis Transactions here
func (r *RedisDataStoreClient) AddDag(ctx context.Context, dag *core.DAG) {
	data, err := dag.MarshalJSON()
	if err != nil {
		panic(err)
	}
	sugar.Debugf("Adding dag %s to store", dag.Name)
	_, err = r.GetClient().HSet(ctx, "dags", dag.Name, data).Result()
	if err != nil {
		panic(err)
	}

	// TODO: Remove old tasks from redis.
	for _, task := range dag.GetTasks() {
		r.AddTask(ctx, task)
	}
}

func taskKeyPrefix(dag *core.DAG) string {
	return dag.Name + ":"
}

func (r *RedisDataStoreClient) AddTask(ctx context.Context, task core.TaskInterface) {
	data, err := task.MarshalJSON()
	if err != nil {
		panic(err)
	}
	sugar.Debugf("Adding task %s:%s to store", task.GetDag().Name, task.GetName())
	_, err = r.GetClient().HSet(ctx, "tasks", taskKeyPrefix(task.GetDag())+task.GetName(), data).Result()
	if err != nil {
		panic(err)
	}
}

func (r *RedisDataStoreClient) AddOrUpdateTaskInstance(ctx context.Context, taskInstance *core.TaskInstance, dag *core.DAG) {
	sugar.Debugf("Adding taskinstance %s:%s with state %d to store",
		dag.Name, taskInstance.TaskName, taskInstance.State)
	key := taskKeyPrefix(dag) + string(taskInstance.Time.Unix())
	_, err := r.GetClient().HSet(ctx, key, taskInstance.TaskName, taskInstance).Result()
	if err != nil {
		panic(err)
	}
}

// func GetTask(taskName string) {}

// func GetAllTasks(dagName string) {}

func (r *RedisDataStoreClient) GetDagLastRun(ctx context.Context, dagName string) (time.Time, bool) {
	val, err := r.GetClient().ZRevRangeByScore(
		ctx,
		"dagruns:"+dagName,
		&redis.ZRangeBy{Min: "-inf", Max: "+inf", Count: 1},
	).Result()
	if err != nil {
		panic(err)
	}
	if len(val) == 0 {
		return time.Time{}, true
	}
	lastRun := core.DagRunType{}
	err = (&lastRun).UnmarshalBinary([]byte(val[0]))
	if err != nil {
		panic(err)
	}
	return time.Unix(lastRun.SchTime, 0), lastRun.Completed
}

// TODO: Do this using redis transaction.
func (r *RedisDataStoreClient) AddDagRun(ctx context.Context, dag *core.DAG, schTime time.Time) {
	dagName := dag.Name
	_, err := r.GetClient().ZAdd(
		ctx,
		"dagruns:"+dagName,
		&redis.Z{Score: float64(schTime.Unix()), Member: &core.DagRunType{schTime.Unix(), false}},
	).Result()
	if err != nil {
		panic(err)
	}
	tasksChan := dag.TopologicalSortedTasks()

	// Add taskruns for all tasks whose downstream is empty with
	// state set to 0 (i.e, not running)
	// TODO: Refactor this, get task instances from a method in core.tasks.
	for task := range tasksChan {
		if len(task.GetDownstream()) != 0 {
			r.AddOrUpdateTaskInstance(ctx, &core.TaskInstance{
				State:    core.TASK_SCHEDULED,
				Time:     schTime,
				TaskName: task.GetName(),
				DagName:  dag.Name,
			}, dag)
		} else {
			r.AddOrUpdateTaskInstance(ctx, &core.TaskInstance{
				State:    core.TASK_READY_TO_RUN,
				Time:     schTime,
				TaskName: task.GetName(),
				DagName:  dag.Name,
			}, dag)
		}
	}
}

func (r *RedisDataStoreClient) GetAllDags(ctx context.Context) chan *core.DAG {
	allDagsChan := make(chan *core.DAG)
	go func() {
		val, err := r.GetClient().HGetAll(ctx, "dags").Result()
		if err != nil {
			panic(err)
		}
		for _, dag := range val {
			println(dag)
			allDagsChan <- r.getDagFromStr(ctx, dag)
		}
		close(allDagsChan)
	}()
	return allDagsChan
}

func (r *RedisDataStoreClient) GetTaskInstances(ctx context.Context, dag *core.DAG, dagRunTime time.Time) map[string]*core.TaskInstance {
	val, err := r.GetClient().HGetAll(ctx, taskKeyPrefix(dag)+string(dagRunTime.Unix())).Result()
	if err != nil {
		panic(err)
	}
	taskInstances := make(map[string]*core.TaskInstance)
	for taskInstanceName, marshaledTaskInstance := range val {
		taskInstance := &core.TaskInstance{}
		err := taskInstance.UnmarshalBinary([]byte(marshaledTaskInstance))
		if err != nil {
			panic(err)
		}
		taskInstances[taskInstanceName] = taskInstance
	}
	return taskInstances
}

func (r *RedisDataStoreClient) GetDag(ctx context.Context, dagName string) *core.DAG {
	val, err := r.GetClient().HGet(ctx, "dags", dagName).Result()
	if err != nil {
		panic(err)
	}
	return r.getDagFromStr(ctx, val)
}

func (r *RedisDataStoreClient) getDagFromStr(ctx context.Context, val string) *core.DAG {
	dag := &core.DAG{}
	err := dag.UnmarshalJSON([]byte(val))
	if err != nil {
		panic(err)
	}
	fetchedTasks := make([]map[string]interface{}, 0)
	for taskName := range dag.GetTasks() {
		val, err := r.GetClient().HGet(ctx, "tasks", taskKeyPrefix(dag)+taskName).Result()
		if err != nil {
			panic(err)
		}
		task := make(map[string]interface{})
		err = json.Unmarshal([]byte(val), &task)
		fetchedTasks = append(fetchedTasks, task)
		if err != nil {
			panic(err)
		}
		core.UnmarhsaledJSONtoTask(task, dag)
		if err != nil {
			panic(err)
		}
	}
	for _, task := range fetchedTasks {
		downstream, ok := task["downstream"].(map[string]interface{})
		taskName := task["name"].(string)
		if ok {
			for downstreamTask := range downstream {
				dag.GetTasks()[taskName].AddDownstream(dag.GetTasks()[downstreamTask])
			}
		}

		upstream, ok := task["upstream"].(map[string]interface{})
		if ok {
			for upstreamTask := range upstream {
				dag.GetTasks()[taskName].AddUpstream(dag.GetTasks()[upstreamTask])
			}
		}

	}
	return dag
}

// func main() {
// 	// Sample DAG check
// 	schedule, _ := time.ParseDuration("2h")
// 	sampleDag := &core.DAG{Name: "SampleDAG", Schedule: schedule, DefaultArgs: core.Dict{}, Description: "", StartDate: time.Now()}
// 	task1, _ := core.NewTask("SampleTask1", "sample description", sampleDag, core.Dict{"1": "2"})
// 	task2, _ := core.NewTask("SampleTask2", "sample description", sampleDag, core.Dict{"1": "2"})
// 	task3, _ := core.NewTask("SampleTask3", "sample description", sampleDag, core.Dict{"1": "2"})
// 	task4, _ := core.NewTask("SampleTask4", "sample description", sampleDag, core.Dict{"1": "2"})
// 	task5, _ := core.NewTask("SampleTask5", "sample description", sampleDag, core.Dict{"1": "2"})
// 	task6, _ := core.NewTask("SampleTask6", "sample description", sampleDag, core.Dict{"1": "2"})
// 	task1.AddUpstream(task2)
// 	task5.AddUpstream(task6)
// 	// task6.AddUpstream(task4)
// 	task4.AddUpstream(task5)
// 	task4.AddUpstream(task1)
// 	task3.Void()
// 	sampleDag.LogInfo()
// 	for task := range sampleDag.TopologicalSortedTasks() {
// 		fmt.Println(task.GetName())
// 	}
// 	r := RedisDataStoreClient{}
// 	r.AddDag(sampleDag)
// 	cachedDag := r.GetDag(sampleDag.Name)
// 	cachedDag.LogInfo()

// 	for task := range cachedDag.TopologicalSortedTasks() {
// 		fmt.Println(task.GetName())
// 	}
// }
