package scheduler

// TODO: Convert Scheduler to HA scheduler, where multiple schedulers running
// on different servers can run and communicate concurrently.

import (
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/AnubhavUjjawal/MoMo/logger"

	"github.com/AnubhavUjjawal/MoMo/config"
	"github.com/AnubhavUjjawal/MoMo/core"
)

// Scheduler parses DAGs and schedules them based on their schedule
type Scheduler struct {
	parseSchedule time.Duration
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
		go core.ParseDag(parseDagChan, parseDagCompleteChan)
	}

	start := time.Now()
	for _, file := range files {
		if filepath.Ext(file.Name()) == config.DagFileExtension {
			parseDagChan <- file
		}
	}
	close(parseDagChan)
	for _, file := range files {
		_ = file
		<-parseDagCompleteChan
	}
	finish := time.Now()
	sugar.Debugf("Took %d ns", finish.Sub(start))
	parseCompleteSignalChan <- struct{}{}
}

type schedulerCommand struct {
	core.Command
}

// RunCommand parses the flags starts the Scheduler.
func (sch *schedulerCommand) RunCommand() error {
	// TODO: parse flags before starting scheduler
	sugar := logger.GetSugaredLogger()
	sugar.Infow("Starting Scheduler")
	sugar.Debugf("Dag parse Interval: %s", config.GetDagsParseInterval())

	parseDuration, err := time.ParseDuration(config.GetDagsParseInterval())
	if err != nil {
		sugar.Fatalw("Failed to parse DAGS. Invalid DAGS_PARSE_INTERVAL",
			"DAGS_PARSE_INTERVAL", config.GetDagsParseInterval())
	}
	scheduler := Scheduler{parseDuration}
	msgChannel := time.NewTicker(scheduler.parseSchedule)

	// TODO: Add heartbeat in parseDagsHandler and run it as a gorountine.
	// TODO: Add dag code scheduler which looks at dag schedule, and adds a
	// 		 corresponding DagRun if a schedule is due.
	scheduler.parseDagsHandler(msgChannel)
	return nil
}

// NewCommand creates and returns an instance of SchedulerCommand.
func NewCommand() *schedulerCommand {
	commandString := "scheduler"
	flagset := flag.NewFlagSet(commandString, flag.ExitOnError)
	return &schedulerCommand{core.Command{CommandString: commandString, Flagset: flagset}}
}
