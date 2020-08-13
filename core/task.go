package core

import "fmt"

type TaskInterface interface {
	AddUpstream(TaskInterface)
	AddDownstream(TaskInterface)
	GetName() string
	GetDescription() string
	GetDag() *DAG
	GetKwargs() Dict
	String() string
	GetDownstream() map[TaskInterface]struct{}
	GetUpstream() map[TaskInterface]struct{}

	// Void does nothing. Call this method to silence errors on tasks in dags.
	Void()
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
	upstream map[TaskInterface]struct{}
	// downstream tasks are those which the current task is dependent
	downstream map[TaskInterface]struct{}
}

// Void does nothing. However this method is necessary to fill the voids in our
// DAGs.
func (task *BaseTask) Void() {

}

func (task *BaseTask) AddUpstream(sibling TaskInterface) {
	if task.upstream == nil {
		task.upstream = make(map[TaskInterface]struct{})
	}
	_, ok := task.upstream[sibling]
	if ok {
		return
	}
	task.upstream[sibling] = struct{}{}
	sibling.AddDownstream(task)
}

func (task *BaseTask) AddDownstream(sibling TaskInterface) {
	if task.downstream == nil {
		task.downstream = make(map[TaskInterface]struct{})
	}
	_, ok := task.downstream[sibling]
	if ok {
		return
	}
	task.downstream[sibling] = struct{}{}
	sibling.AddUpstream(task)
}

func (task *BaseTask) GetDownstream() map[TaskInterface]struct{} {
	return task.downstream
}

func (task *BaseTask) GetUpstream() map[TaskInterface]struct{} {
	return task.upstream
}

func (task *BaseTask) GetName() string        { return task.name }
func (task *BaseTask) GetDescription() string { return task.description }
func (task *BaseTask) GetDag() *DAG           { return task.dag }
func (task *BaseTask) GetKwargs() Dict        { return task.kwargs }
func (task *BaseTask) String() string         { return task.GetName() }

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
	dag.tasks[newTask.GetName()] = &newTask
	return &newTask, nil
}
