package core

import (
	"fmt"
	"os"
	"time"

	"github.com/AnubhavUjjawal/MoMo/logger"
)

// Dict type, can be used to hold arbitrary data.
type Dict map[interface{}]interface{}

// DAG is a basic collection of tasks that we want to run, organised in a way
// that specifies their dependencies and relationships.
type DAG struct {
	Name        string
	Schedule    time.Duration
	DefaultArgs Dict
	Description string
	tasks       map[string]TaskInterface
}

func (dag *DAG) DetectCycles() error {
	unvisitedSet := make(map[string]struct{})
	visitingSet := make(map[string]struct{})
	visitedSet := make(map[string]struct{})
	for taskName, _ := range dag.tasks {
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

func (dag *DAG) detectCycleDFS(nextEl string, unvisitedSet, visitingSet, visitedSet *map[string]struct{}) bool {
	// move nextEl from unvisited to visiting
	delete(*unvisitedSet, nextEl)
	(*visitingSet)[nextEl] = struct{}{}

	// iterate over neighbors and call detectCycleDFS on them if they are not
	// already visited.
	for neighbor := range dag.tasks[nextEl].GetUpstream() {
		_, ok := (*visitedSet)[neighbor.GetName()]
		// neighbor already visited
		if ok {
			continue
		}
		_, ok = (*visitingSet)[neighbor.GetName()]
		// cycle detected
		if ok {
			sugar := logger.GetSugaredLogger()
			sugar.Infow("Cycle detected b/w nodes", "nodes", visitingSet)
			return true
		}

		if dag.detectCycleDFS(neighbor.GetName(), unvisitedSet, visitingSet, visitedSet) {
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
		for len(taskQ) > 0 {
			task := taskQ[0]
			taskQ = taskQ[1:]
			for upStreamTask := range dag.tasks[task.GetName()].GetUpstream() {
				inDegree[upStreamTask.GetName()]--
				if inDegree[upStreamTask.GetName()] == 0 {
					taskQ = append(taskQ, upStreamTask)
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

// ParseDag parses and looks for DAGS in DAG files recieved using the passed
// channel.
func ParseDag(parseDagChan <-chan os.FileInfo) {
	// dagsDir := config.GetDagsDir()
	for fileInfo := range parseDagChan {
		fmt.Println(fileInfo.Name())
		// simulating expensive operation using time.Sleep()
		workDuration, _ := time.ParseDuration("1000us")
		time.Sleep(workDuration)
	}
}
