package core

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

type TaskInterface interface {
	AddUpstream(TaskInterface)
	AddDownstream(TaskInterface)
	GetName() string
	GetDescription() string
	GetDag() *DAG
	GetKwargs() Dict
	String() string
	GetDownstream() map[string]struct{}
	GetUpstream() map[string]struct{}
	MarshalJSON() ([]byte, error)
	// Void does nothing. Call this method to silence errors on tasks in dags.
	Void()
	Run()
}

type taskState int8

const TASK_SCHEDULED taskState = 0
const TASK_READY_TO_RUN taskState = 1
const TASK_RUNNING taskState = 2
const TASK_COMPLETED taskState = 3
const TASK_FAILED taskState = 4

// TODO: Add Execution Start Time
type TaskInstance struct {
	// 0 for scheduled, 1 for running, 2 for completed.
	State    taskState
	Time     time.Time
	TaskName string
	DagName  string
}

func (taskIn *TaskInstance) MarshalBinary() ([]byte, error) {
	return json.Marshal(taskIn)
}

func (taskIn *TaskInstance) UnmarshalBinary(val []byte) error {
	return json.Unmarshal(val, taskIn)
}

// BaseTask contains the basic task attributes, and implements the basic
// methods that can be used. After deriving this struct using composition,
// you are expected to implement another `NewTask` and the `Execute` method.
// NOTE: Always use the `NewTask` method to create an instance of this struct.
type BaseTask struct {
	// Task Names should be unique within a same DAG.
	name        string
	description string
	dag         *DAG
	kwargs      Dict
	// upstream tasks are those which are dependent on current task
	upstream map[string]struct{}
	// downstream tasks are those which the current task is dependent
	downstream map[string]struct{}
	taskType   string
}

// Void does nothing. However this method is necessary to fill the voids in our
// DAGs.
func (task *BaseTask) Void() {}

func (task *BaseTask) Run() {
	fmt.Println("Running base task")
}

func (task *BaseTask) GetRegistryName() string {
	// return reflect.TypeOf(op)
	return "BaseTask"
}

func (task *BaseTask) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":        task.GetName(),
		"description": task.GetDescription(),
		"kwargs":      task.GetKwargs(),
		"upstream":    task.GetUpstream(),
		"downstream":  task.GetDownstream(),
		"type":        task.taskType,
	})
}

func Invoke(any reflect.Value, name string, args ...interface{}) {
	inputs := make([]reflect.Value, len(args))
	for i := range args {
		inputs[i] = reflect.ValueOf(args[i])
	}
	any.MethodByName(name).Call(inputs)
}

// UnmarhsaledJSONtoTask converts output of MarshalJSON() to a task. Make sure
// to add the task to the passed dag.tasks as well.
func UnmarhsaledJSONtoTask(taskData map[string]interface{}, dag *DAG) {
	// add recover here, as not setting type properly leads to panic
	// TODO: Allow other Tasks by plugins.
	// TODO: This method needs rework.
	// TODO: Use PkgPath along with type in switch to accurately recreate struct.
	// reflect.TypeOf("x").ConvertibleTo()
	// fmt.Println(OperatorsRegistry[taskData["type"].(string)])
	newTask := reflect.New(OperatorsRegistry[taskData["type"].(string)])
	// fmt.Println(newTask.Type(), "yooyoy")
	Invoke(newTask, "SetName", taskData["name"].(string))
	Invoke(newTask, "SetDescription", taskData["description"].(string))
	Invoke(newTask, "SetKwargs", Dict(taskData["kwargs"].(map[string]interface{})))
	Invoke(newTask, "SetDag", dag)
	Invoke(newTask, "SetTaskType", taskData["type"].(string))
	// fmt.Println(newTask.MethodByName("GetName").Call([]reflect.Value{}))
	// newTask := &BaseTask{
	// 	name:        taskData["name"].(string),
	// 	description: taskData["description"].(string),
	// 	kwargs:      Dict(taskData["kwargs"].(map[string]interface{})),
	// 	dag:         dag,
	// }
	// dag.tasks[newTask.GetName()] = newTask.(TaskInterface)
	// typeOfOp := OperatorsRegistry[taskData["type"].(string)]
	// newTask.Interface().(typeOfOp)
	// newTask.FieldByName("name").SetString(taskData["name"].(string))
	// newTask.FieldByName("description").SetString(taskData["description"].(string))
	// newTask.FieldByName("kwargs").Set(reflect.ValueOf(Dict(taskData["kwargs"].(map[string]interface{}))))
	// newTask.FieldByName("dag").Set(reflect.ValueOf(dag))
	// name := []reflect.ValueOf()
	// newTask.MethodByName("SetName").Call({reflect.ValueOf(taskData["name"].(string))})
	dag.tasks[taskData["name"].(string)] = newTask.Interface().(TaskInterface)
}

func (task *BaseTask) AddUpstream(sibling TaskInterface) {
	if task.upstream == nil {
		task.upstream = make(map[string]struct{})
	}
	_, ok := task.upstream[sibling.GetName()]
	if ok {
		return
	}
	task.upstream[sibling.GetName()] = struct{}{}
	sibling.AddDownstream(task)
}

func (task *BaseTask) AddDownstream(sibling TaskInterface) {
	if task.downstream == nil {
		task.downstream = make(map[string]struct{})
	}
	_, ok := task.downstream[sibling.GetName()]
	if ok {
		return
	}
	task.downstream[sibling.GetName()] = struct{}{}
	sibling.AddUpstream(task)
}

func (task *BaseTask) GetDownstream() map[string]struct{} {
	return task.downstream
}

func (task *BaseTask) GetUpstream() map[string]struct{} {
	return task.upstream
}

func (task *BaseTask) GetName() string        { return task.name }
func (task *BaseTask) GetDescription() string { return task.description }
func (task *BaseTask) GetDag() *DAG           { return task.dag }
func (task *BaseTask) GetKwargs() Dict        { return task.kwargs }
func (task *BaseTask) String() string         { return task.GetName() }

func (task *BaseTask) SetName(name string) {
	// fmt.Println("here")
	task.name = name
}
func (task *BaseTask) SetDescription(description string) {
	task.description = description
}
func (task *BaseTask) SetDag(dag *DAG) {
	task.dag = dag
}
func (task *BaseTask) SetKwargs(kwargs Dict) {
	task.kwargs = kwargs
}
func (task *BaseTask) SetTaskType(taskType string) {
	task.taskType = taskType
}

// Only for Operators which inherit BaseTask.
func (task *BaseTask) SetMetadata(name string, description string, dag *DAG, kwargs Dict, taskType string) {
	task.name = name
	task.description = description
	task.dag = dag
	task.kwargs = kwargs
	task.taskType = taskType
}

func NewTask(name string, description string, dag *DAG, kwargs Dict) (TaskInterface, error) {
	if dag.tasks == nil {
		dag.tasks = make(map[string]TaskInterface)
	}
	_, ok := dag.tasks[name]
	if ok {
		return nil, fmt.Errorf("invalid task name. task with name %s already exists", name)
	}

	newTask := BaseTask{
		name:        name,
		description: description,
		dag:         dag,
		kwargs:      kwargs}
	newTask.taskType = newTask.GetRegistryName()
	dag.tasks[newTask.GetName()] = &newTask
	return &newTask, nil
}

// func (task *BaseTask) NewTask()

var OperatorsRegistry map[string]reflect.Type = map[string]reflect.Type{}
