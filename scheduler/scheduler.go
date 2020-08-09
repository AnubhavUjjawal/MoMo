package scheduler

// TODO: Convert Scheduler to HA scheduler, where multiple schedulers running
// on different servers can run and communicate concurrently.

import (
	"flag"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/AnubhavUjjawal/MoMo/config"
	"github.com/AnubhavUjjawal/MoMo/core"
	"go.uber.org/zap"
)

// Scheduler parses DAGs and schedules them based on their schedule
type Scheduler struct {
	parseSchedule time.Duration
	logger        *zap.SugaredLogger
}

// ParseDagsHandler handles creation and utilization of resources for ParseDags.
func (sch *Scheduler) parseDagsHandler(ticker *time.Ticker) {
	// TODO: Debounce next parse till previous one has completed.
	dagsDir := config.GetDagsDir()
	osSignal := make(chan os.Signal)
	signal.Notify(osSignal, syscall.SIGINT)
	sch.logger.Infow("Starting parsing dags at", "path", dagsDir)

	go sch.parseDags(dagsDir)
	for {
		select {
		case <-ticker.C:
			go sch.parseDags(dagsDir)
		case <-osSignal:
			defer os.Exit(0)
			sch.logger.Infow("Gracefully stopping parsing dags at", "path", dagsDir)
			ticker.Stop()
			runtime.Goexit()
		}
	}
}

func (sch *Scheduler) parseDags(dagsDir string) {
	// TODO: Implement this method.
	sch.logger.Infow("Parsing dags at", "path", dagsDir)
}

type SchedulerCommand struct {
	core.Command
}

// RunCommand parses the flags starts the Scheduler.
func (sch *SchedulerCommand) RunCommand(logger *zap.SugaredLogger) error {
	// TODO: parse flags before starting scheduler
	logger.Infow("Starting Scheduler")

	parseDuration, _ := time.ParseDuration("5m")
	scheduler := Scheduler{parseDuration, logger}
	msgChannel := time.NewTicker(scheduler.parseSchedule)
	scheduler.parseDagsHandler(msgChannel)
	return nil
}

// NewCommand creates and returns an instance of schedulerCommand.
func NewCommand() *SchedulerCommand {
	commandString := "scheduler"
	flagset := flag.NewFlagSet(commandString, flag.ExitOnError)
	return &SchedulerCommand{core.Command{CommandString: commandString, Flagset: flagset}}
}
