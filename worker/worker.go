package worker

import (
	"context"
	"flag"

	"github.com/AnubhavUjjawal/MoMo/config"
	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/AnubhavUjjawal/MoMo/logger"
	"github.com/AnubhavUjjawal/MoMo/pubsub"
	"github.com/AnubhavUjjawal/MoMo/storage"
)

type workerCommand struct {
	core.Command
}

type taskInstanceMessage struct {
	id string
	ti *core.TaskInstance
}

// TODO: Add Graceful exit.
func executeTask(taskInstanceChan <-chan *taskInstanceMessage) {
	sugar := logger.GetSugaredLogger()
	pubsubClient := pubsub.PubSubClient()
	storeClient := storage.DataStoreClient()
	for tim := range taskInstanceChan {
		sugar.Infow("Starting to run:",
			"taskName", tim.ti.TaskName,
			"msgid", tim.id,
			"dagName", tim.ti.DagName,
			"schTime", tim.ti.Time)
		dag := storeClient.GetDag(context.TODO(), tim.ti.DagName)
		// Move task instance to running.
		tim.ti.State = core.TASK_RUNNING
		storeClient.AddOrUpdateTaskInstance(context.TODO(), tim.ti, dag)
		// Execute task here.
		// Move task instance to completed.
		tim.ti.State = core.TASK_COMPLETED
		storeClient.AddOrUpdateTaskInstance(context.TODO(), tim.ti, dag)
		pubsubClient.PublishTaskInstanceAsComplete(context.TODO(), tim.ti, dag)
		// Acknowledge that the task is complete.
		pubsubClient.AckTaskProcessed(context.TODO(), tim.id)
	}
}

func startWorker() {
	sugar := logger.GetSugaredLogger()
	numWorkerRoutines := config.GetNumWorkers()
	sugar.Infow("Starting Workers 👷👷", "numWorkerRoutines", numWorkerRoutines)
	tasksInstanceChan := make(chan *taskInstanceMessage, numWorkerRoutines)
	// TODO: Implement heartbeart to make sure numWorkerRoutines executeTask are
	// always running.
	for i := 0; i < numWorkerRoutines; i++ {
		go executeTask(tasksInstanceChan)
	}
	pubsubClient := pubsub.PubSubClient()
	pubsubClient.CreateConsumerGroupTasks(context.TODO())
	for {
		tis, ids := pubsubClient.GetReadyToRunTasks(context.TODO(), 1)
		// fmt.Println(tis, dagNames)
		// push task into the channel here.
		for index, ti := range tis {
			tasksInstanceChan <- &taskInstanceMessage{ti: ti, id: ids[index].(string)}
		}
	}
}

// RunCommand parses the flags starts the Worker.
func (wch *workerCommand) RunCommand() error {
	// TODO: parse flags before starting Worker
	startWorker()
	return nil
}

// NewCommand creates and returns an instance of workerCommand.
func NewCommand() *workerCommand {
	commandString := "worker"
	flagset := flag.NewFlagSet(commandString, flag.ExitOnError)
	return &workerCommand{core.Command{CommandString: commandString, Flagset: flagset}}
}
