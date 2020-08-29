package scheduler

// TODO: Convert Scheduler to HA scheduler, where multiple schedulers running
// on different servers can run and communicate concurrently.

import (
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"plugin"
	"runtime"
	"syscall"
	"time"

	"github.com/AnubhavUjjawal/MoMo/logger"
	"github.com/AnubhavUjjawal/MoMo/storage"

	"github.com/AnubhavUjjawal/MoMo/config"
	"github.com/AnubhavUjjawal/MoMo/core"
)

// GetDAGsFn is the name of the function which is called when we open a dag
// file. The dags returned from that fn are scheduled.
const GetDAGsFn = "GetDAGs"

// Scheduler parses DAGs and schedules them based on their schedule
type Scheduler struct {
	parseSchedule time.Duration

	// runSchedule is the timeDuration after which the scheduler must check
	// again for any tasks which need to be scheduled.
	runSchedule time.Duration
}

// ParseDagsHandler handles creation and utilization of resources for ParseDags.
func (sch *Scheduler) parseDagsHandler(ticker *time.Ticker) {
	sugar := logger.GetSugaredLogger()
	dagsDir := config.GetDagsDir()
	osSignal := make(chan os.Signal)
	// buffering by 1 so that sch.parseDags can exit without blocking.
	parseCompleteSignalChan := make(chan struct{}, 1)
	signal.Notify(osSignal, syscall.SIGINT)
	sugar.Infow("Starting parsing dags at", "path", dagsDir)

	// TODO: Add recover from panic if any spawned goroutine fails.
	// Ex: Add recover when connection to store is refused.
	prevParseComplete := false
	go sch.parseDags(parseCompleteSignalChan)
	for {
		select {
		case <-ticker.C:
			if !prevParseComplete {
				sugar.Infow("Skipping dags parsing. Previous parse yet not complete.")
			} else {
				prevParseComplete = false
				go sch.parseDags(parseCompleteSignalChan)
			}
		case <-osSignal:
			defer os.Exit(0)
			sugar.Infow("Gracefully stopping parsing dags at", "path", dagsDir)
			ticker.Stop()
			runtime.Goexit()
		case <-parseCompleteSignalChan:
			prevParseComplete = true
		}
	}
}

func (sch *Scheduler) parseDags(parseCompleteSignalChan chan<- struct{}) {
	// https://blog.golang.org/pipelines
	dagsDir := config.GetDagsDir()
	sugar := logger.GetSugaredLogger()
	sugar.Infow("Parsing dags at", "path", dagsDir)

	files, err := ioutil.ReadDir(dagsDir)
	if err != nil {
		sugar.Errorw("Got err while trying to parseDags", "dagsDir", dagsDir, "err", err)
		return
	}

	numConcurrentWorkers := config.NumCocurrencyGoRoutine()
	parseDagChan := make(chan os.FileInfo, numConcurrentWorkers)
	parseDagCompleteChan := make(chan struct{})

	for i := 0; i < numConcurrentWorkers; i++ {
		sugar.Debugf("Spawned %d goroutine to parse dags", i+1)
		go sch.parseDag(parseDagChan, parseDagCompleteChan)
	}

	start := time.Now()
	for _, file := range files {
		if filepath.Ext(file.Name()) == config.DagFileExtension {
			parseDagChan <- file
		}
	}
	close(parseDagChan)
	for range files {
		<-parseDagCompleteChan
	}
	finish := time.Now()
	sugar.Debugf("Took %d ns", finish.Sub(start))
	parseCompleteSignalChan <- struct{}{}
}

// parseDag parses and looks for DAGS in DAG files recieved using the passed
// channel, and creates DagRuns according to their Schedule and StartTime.
func (sch *Scheduler) parseDag(parseDagChan <-chan os.FileInfo, parseDagCompleteChan chan<- struct{}) {
	sugar := logger.GetSugaredLogger()
	dagsDir := config.GetDagsDir()

	// TODO: Add contextual deadline per fileInfo.
	for fileInfo := range parseDagChan {
		defer func() { parseDagCompleteChan <- struct{}{} }()
		p, err := plugin.Open(path.Join(dagsDir, fileInfo.Name()))
		if err != nil {
			sugar.Errorw("Error while opening file as plugin", "err", err, "file", fileInfo.Name())
			continue
		}
		fn, err := p.Lookup(GetDAGsFn)
		if err != nil {
			sugar.Errorf("Could not find %s fn in plugin file %s", GetDAGsFn, fileInfo.Name())
			continue
		}
		getDags, ok := fn.(func() []*core.DAG)
		if !ok {
			sugar.Errorf("Error while typecasting fn %s in plugin file %s", GetDAGsFn, fileInfo.Name())
			continue
		}

		// TODO: Add a recover for getDags()
		dags := getDags()
		storageClient := storage.DataStoreClient()
		for _, dag := range dags {
			sugar.Infof("Sending dag %s to store", dag.Name)
			storageClient.AddDag(dag)

			// For a DAG to be scheduled, these conditions must be met:
			// - start date < curr time
			// - previousDagRun is complete
			// - curr time > previousDagRun + dag schedule
			now := time.Now()
			lastRun, lastRunIsComplete := storageClient.GetDagLastRun(dag.Name)
			sugar.Debugw("Should DAG be scheduled",
				"DagStartDateBeforeNow", dag.StartDate.Before(now),
				"LastRunPlusScheduleBeforeNow", lastRun.Add(dag.Schedule).Before(now),
				"LastRunComplete", lastRunIsComplete)
			if dag.StartDate.Before(now) &&
				lastRun.Before(now.Add(dag.Schedule)) &&
				lastRunIsComplete {
				sugar.Infof("Adding %s dag to scheduled run %s", dag.Name, now)
				// push tasks to pubsub then AddDagRun to storage client.
				storageClient.AddDagRun(dag, now)
			}
		}
	}
}

func (sch *Scheduler) runTasks(tasksChan chan<- core.TaskInterface) {

}

func (sch *Scheduler) scheduleTasks(dag *core.DAG, lastRunTime time.Time, tasksChan <-chan core.TaskInterface) {
	// TODO: Check if tasks are already running.
	storageClient := storage.DataStoreClient()
	taskRuns := storageClient.GetTaskRuns(dag.Name, lastRunTime)
	tasksRunOrderChan := dag.TopologicalSortedTasks()
	if len(taskRuns) == 0 {
		_ = <-tasksRunOrderChan
		// publish this task to redis pub sub
	}

}

// func (sch *Scheduler) runDags() {
// 	// For all DagRun currently incomplete(false), see what task instances are completed.
// 	// If no task instance is completed, run the first task instance.
// 	// Else, run the next task in sequence.

// 	// TODO: Make this more concurrent

// 	sugar := logger.GetSugaredLogger()
// 	storageClient := storage.DataStoreClient()
// 	for dag := range storageClient.GetAllDags() {
// 		lastRunTime, lastRunIsComplete := storageClient.GetDagLastRun(dag.Name)
// 		if lastRunIsComplete {
// 			continue
// 		}
// 		sugar.Debugw("Last run time for DAG", "dag", dag.Name, "time", lastRunTime.String())

// 		tasksChan := make(chan core.TaskInterface)
// 		go sch.runTasks(tasksChan)
// 		go sch.scheduleTasks(dag, lastRunTime, tasksChan)
// 	}
// }

type schedulerCommand struct {
	core.Command
}

// RunCommand parses the flags starts the Scheduler.
func (sch *schedulerCommand) RunCommand() error {
	// TODO: parse flags before starting scheduler
	sugar := logger.GetSugaredLogger()
	sugar.Infow("Starting Scheduler")
	sugar.Debugf("Dag parse Interval: %s", config.GetDagsParseInterval())
	parseDuration := config.GetDagsParseInterval()
	runDuration := config.GetDagRunCheckInterval()
	scheduler := Scheduler{parseDuration, runDuration}
	msgChannelParseDagsHandler := time.NewTicker(scheduler.parseSchedule)
	msgChannelRunDags := time.NewTicker(scheduler.runSchedule)

	// TODO: Add heartbeat to both goroutines so that we can respawn failed ones.
	go func() {
		for range msgChannelRunDags.C {
			scheduler.scheduleTasks()
		}
	}()
	scheduler.parseDagsHandler(msgChannelParseDagsHandler)
	return nil
}

// NewCommand creates and returns an instance of SchedulerCommand.
func NewCommand() *schedulerCommand {
	commandString := "scheduler"
	flagset := flag.NewFlagSet(commandString, flag.ExitOnError)
	return &schedulerCommand{core.Command{CommandString: commandString, Flagset: flagset}}
}
