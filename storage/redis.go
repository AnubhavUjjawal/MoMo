package redis

// package main

import (
	"context"
	"encoding/json"

	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/go-redis/redis/v8"
)

var redisClient *redis.Client
var ctx = context.Background()

// RedisClient returns a redis.Client
func RedisClient() *redis.Client {
	// TODO: Pickup, Addr, Password, DB, Username, PoolSize through env variables.
	if redisClient != nil {
		return redisClient
	}
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
		PoolSize: 16,
	})
	return redisClient
}

//TODO: get ctx passed in methods which interact with RedisClient
// TODO: store dag and tasks separately
func AddDag(dag *core.DAG) {
	data, err := dag.MarshalJSON()
	if err != nil {
		panic(err)
	}
	_, err = RedisClient().HSet(ctx, "dags", dag.Name, data).Result()
	if err != nil {
		panic(err)
	}

	// TODO: Remove old tasks from redis.

	// add tasks to redis tasks Hset as well
	for _, task := range dag.GetTasks() {
		AddTask(task)
	}
}

func taskKeyPrefix(dag *core.DAG) string {
	return dag.Name + ":"
}

// store TaskType when serialising as well.
func AddTask(task core.TaskInterface) {
	data, err := task.MarshalJSON()
	if err != nil {
		panic(err)
	}
	_, err = RedisClient().HSet(ctx, "tasks", taskKeyPrefix(task.GetDag())+task.GetName(), data).Result()
	if err != nil {
		panic(err)
	}
}

func GetTask(taskName string) {}

func GetAllTasks(dagName string) {}

func GetDag(dagName string) *core.DAG {
	val, err := RedisClient().HGet(ctx, "dags", dagName).Result()
	if err != nil {
		panic(err)
	}
	dag := &core.DAG{}
	err = dag.UnmarshalJSON([]byte(val))
	if err != nil {
		panic(err)
	}

	fetchedTasks := make([]map[string]interface{}, 0)
	for taskName := range dag.GetTasks() {
		val, err := RedisClient().HGet(ctx, "tasks", taskKeyPrefix(dag)+taskName).Result()
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

// 	AddDag(sampleDag)
// 	cachedDag := GetDag(sampleDag.Name)
// 	cachedDag.LogInfo()

// 	for task := range cachedDag.TopologicalSortedTasks() {
// 		fmt.Println(task.GetName())
// 	}
// }
