package core

import (
	"errors"
	"flag"

	"go.uber.org/zap"
)

// CommandInterface is set of methods that need to be implemented
// for packages that want to expose a command for the command line.
type CommandInterface interface {
	RunCommand(*zap.SugaredLogger) error
	GetCommandString() string
	GetFlagSet() *flag.FlagSet
}

// Command is a struct view of a commandline command.
type Command struct {
	CommandString string
	Flagset       *flag.FlagSet
}

func (comm *Command) RunCommand(*zap.SugaredLogger) error {
	return errors.New("Not Implemented")
}

func (comm *Command) GetCommandString() string {
	return comm.CommandString
}

func (comm *Command) GetFlagSet() *flag.FlagSet {
	return comm.Flagset
}
