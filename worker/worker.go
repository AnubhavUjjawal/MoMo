package worker

import (
	"flag"

	"github.com/AnubhavUjjawal/MoMo/logger"

	"github.com/AnubhavUjjawal/MoMo/core"
)

type workerCommand struct {
	core.Command
}

// RunCommand parses the flags starts the Worker.
func (sch *workerCommand) RunCommand() error {
	// TODO: parse flags before starting Worker
	sugar := logger.GetSugaredLogger()
	sugar.Infow("Starting Worker")
	return nil
}

// NewCommand creates and returns an instance of workerCommand.
func NewCommand() *workerCommand {
	commandString := "worker"
	flagset := flag.NewFlagSet(commandString, flag.ExitOnError)
	return &workerCommand{core.Command{CommandString: commandString, Flagset: flagset}}
}
