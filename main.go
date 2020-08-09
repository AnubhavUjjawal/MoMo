package main

import (
	"flag"
	"os"

	"github.com/AnubhavUjjawal/MoMo/server"

	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/AnubhavUjjawal/MoMo/scheduler"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any
	sugar := logger.Sugar()
	sugar.Info("Starting MoMo: A Workflow management platform inspired by Apache Airflow!")

	// Sample DAG check
	// schedule, _ := time.ParseDuration("2h")
	// sampleDag := &dag.DAG{Name: "SampleDAG", Schedule: schedule}
	// sampleDag.LogInfo(sugar)

	if len(os.Args) < 2 {
		sugar.Fatalw("Failed to provide subcommand.")
	}

	commands := registerSubCommands(sugar)
	parseSubCommand(sugar, commands)
}

func registerSubCommands(sugar *zap.SugaredLogger) []core.CommandInterface {
	// TODO: Allow users to add their own custom commands.
	commands := make([]core.CommandInterface, 0)
	commands = append(commands, scheduler.NewCommand(), server.NewCommand())
	return commands
}

func parseSubCommand(sugar *zap.SugaredLogger, commands []core.CommandInterface) {

	success := false
	for _, command := range commands {
		if command.GetCommandString() == os.Args[1] {
			command.RunCommand(sugar)
			success = true
		}
	}
	if !success {
		sugar.Fatalw("Invalid subcommand.")
		flag.PrintDefaults()
		os.Exit(1)
	}
}
