package core

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/AnubhavUjjawal/MoMo/logger"
)

// Dict type, can be used to hold arbitrary data.
type Dict map[string]interface{}

// DAG is a basic collection of tasks that we want to run, organised in a way
// that specifies their dependencies and relationships.
type DAG struct {
	Name        string
	Schedule    time.Duration
	DefaultArgs Dict
	Description string
	StartDate   time.Time
	tasks       map[string]TaskInterface
}

type DagRunType struct {
	SchTime   int64
	Completed bool
}

func (dr *DagRunType) MarshalBinary() ([]byte, error) {
	return json.Marshal(dr)
}

func (dr *DagRunType) UnmarshalBinary(val []byte) error {
	return json.Unmarshal(val, dr)
}

func (dag *DAG) GetTasks() map[string]TaskInterface {
	return dag.tasks
}

func (dag *DAG) DetectCycles() error {
	unvisitedSet := make(map[string]struct{})
	visitingSet := make(map[string]struct{})
	visitedSet := make(map[string]struct{})
	for taskName := range dag.tasks {
		unvisitedSet[taskName] = struct{}{}
	}

	var nextEl string
	for len(unvisitedSet) > 0 {
		for el := range unvisitedSet {
			nextEl = el
			break
		}
		if dag.detectCycleDFS(nextEl, &unvisitedSet, &visitingSet, &visitedSet) {
			return fmt.Errorf("cycle in DAG %s", dag.Name)
		}
	}
	return nil
}

func (dag *DAG) MarshalJSON() ([]byte, error) {
	tasks := make([]string, 0)
	for task := range dag.tasks {
		tasks = append(tasks, task)
	}
	return json.Marshal(map[string]interface{}{
		"Name":        dag.Name,
		"Schedule":    dag.Schedule,
		"DefaultArgs": dag.DefaultArgs,
		"Description": dag.Description,
		"StartDate":   dag.StartDate,
		"tasks":       tasks,
	})
}

// UnmarshalJSON contains the logic of Unmarshaling the dag marshaled by
// MarshalDag. Check BaseTask.MarshalTask as well.
func (dag *DAG) UnmarshalJSON(b []byte) error {
	// sugar := logger.GetSugaredLogger()
	var data map[string]interface{}
	err := json.Unmarshal(b, &data)
	if err != nil {
		return err
	}
	dag.Name = data["Name"].(string)
	dag.Schedule = time.Duration(data["Schedule"].(float64))
	dag.DefaultArgs = Dict(data["DefaultArgs"].(map[string]interface{}))
	dag.Description = data["Description"].(string)
	dag.StartDate, err = time.Parse(time.RFC3339, data["StartDate"].(string))
	dag.tasks = make(map[string]TaskInterface)
	if err != nil {
		panic(err)
	}
	tasks := data["tasks"].([]interface{})
	for _, task := range tasks {
		task := task.(string)
		dag.tasks[task] = nil
	}
	return nil
}

func (dag *DAG) detectCycleDFS(nextEl string, unvisitedSet, visitingSet, visitedSet *map[string]struct{}) bool {
	// move nextEl from unvisited to visiting
	delete(*unvisitedSet, nextEl)
	(*visitingSet)[nextEl] = struct{}{}

	// iterate over neighbors and call detectCycleDFS on them if they are not
	// already visited.
	for neighbor := range dag.tasks[nextEl].GetUpstream() {
		_, ok := (*visitedSet)[neighbor]
		// neighbor already visited
		if ok {
			continue
		}
		_, ok = (*visitingSet)[neighbor]
		// cycle detected
		if ok {
			sugar := logger.GetSugaredLogger()
			sugar.Infow("Cycle detected b/w nodes", "nodes", visitingSet)
			return true
		}

		if dag.detectCycleDFS(neighbor, unvisitedSet, visitingSet, visitedSet) {
			return true
		}
	}

	// move nextEl from visiting to visited
	delete(*visitingSet, nextEl)
	(*visitedSet)[nextEl] = struct{}{}
	return false
}

// TopologicalSortedTasks : Modify this so that it sends out new tasks in
// a channel on updates of finished tasks from a channel received in function param.
func (dag *DAG) TopologicalSortedTasks() chan TaskInterface {
	if err := dag.DetectCycles(); err != nil {
		panic("Cannot do TopologicalSort, dag has a cycle.")
	}

	taskQ := make([]TaskInterface, 0)
	taskChan := make(chan TaskInterface)
	inDegree := make(map[string]int)
	for taskName, task := range dag.tasks {
		inDegree[taskName] = len(task.GetDownstream())
	}

	// Find the tasks which have no downstreams. Those are the tasks which can
	// run first.
	for task := range inDegree {
		if inDegree[task] == 0 {
			taskQ = append(taskQ, dag.tasks[task])
		}
	}

	go func() {

		// This recover func is useful if we want to see the starting
		// n tasks only. Then we can close the taskChan and recover from panic.
		defer func() {
			if r := recover(); r != nil {
			}
		}()
		for len(taskQ) > 0 {
			task := taskQ[0]
			taskQ = taskQ[1:]
			for upStreamTaskName := range dag.tasks[task.GetName()].GetUpstream() {
				inDegree[upStreamTaskName]--
				if inDegree[upStreamTaskName] == 0 {
					taskQ = append(taskQ, dag.tasks[upStreamTaskName])
				}
			}
			taskChan <- task
		}
		close(taskChan)
	}()
	return taskChan
}

// LogInfo logs DAG info.
func (dag *DAG) LogInfo() {
	sugar := logger.GetSugaredLogger()
	tasks := make([]string, 0)
	for task := range dag.tasks {
		tasks = append(tasks, task)
	}
	sugar.Infow("DAG info:", "DAG", dag.Name, "schedule", dag.Schedule, "tasks", tasks)
}
