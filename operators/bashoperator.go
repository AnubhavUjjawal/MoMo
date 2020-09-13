package operators

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"

	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/AnubhavUjjawal/MoMo/logger"
)

type BashOperator struct {
	core.BaseTask
}

func (op *BashOperator) GetRegistryName() string {
	// return reflect.TypeOf(op)
	return "BashOP"
}

func (op *BashOperator) Run() {
	sugar := logger.GetSugaredLogger()
	dir, err := ioutil.TempDir("", "bash_op")
	if err != nil {
		sugar.Fatal(err)
	}
	defer os.RemoveAll(dir)

	file, err := ioutil.TempFile(dir, op.GetName())
	if err != nil {
		sugar.Fatal(err)
	}
	defer os.Remove(file.Name())

	file.Write([]byte(op.GetKwargs()["bash_command"].(string)))
	err = file.Chmod(os.FileMode(int(0777)))
	if err != nil {
		panic(err)
	}
	file.Close()
	sugar.Infof("Temp script location %s", dir)
	out, err := exec.Command("/bin/sh", file.Name()).Output()

	// cmd := &exec.Cmd{
	// 	Path:   file.Name(),
	// 	Stdout: os.Stdout,
	// 	Stderr: os.Stderr,
	// }

	// err = cmd.Run()
	sugar.Info(string(out))
	if err != nil {
		sugar.Fatal(err)
	}
}

func (op *BashOperator) NewTask(name string, description string, dag *core.DAG, bash_command string) (core.TaskInterface, error) {
	if dag.GetTasks() == nil {
		dag.SetTasks(make(map[string]core.TaskInterface))
	}
	_, ok := dag.GetTasks()[name]
	if ok {
		return nil, fmt.Errorf("invalid task name. task with name %s already exists", name)
	}

	kwargs := core.Dict{"bash_command": bash_command}
	newTask := BashOperator{}
	newTask.SetMetadata(name, description, dag, kwargs, newTask.GetRegistryName())
	dag.GetTasks()[newTask.GetName()] = &newTask
	return &newTask, nil
}
