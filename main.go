package main

import (
	"flag"
	"os"

	"github.com/AnubhavUjjawal/MoMo/worker"

	"github.com/AnubhavUjjawal/MoMo/logger"
	"github.com/AnubhavUjjawal/MoMo/server"

	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/AnubhavUjjawal/MoMo/scheduler"
)

func main() {
	sugar := logger.GetSugaredLogger()
	sugar.Info("Starting MoMo 😽😺: A Workflow management platform inspired by Apache Airflow!")

	// Sample DAG check
	// schedule, _ := time.ParseDuration("2h")
	// sampleDag := &core.DAG{Name: "SampleDAG", Schedule: schedule}
	// task1, _ := core.NewTask("SampleTask1", "sample description", sampleDag, core.Dict{"1": "2"})
	// task2, _ := core.NewTask("SampleTask2", "sample description", sampleDag, core.Dict{"1": "2"})
	// task3, _ := core.NewTask("SampleTask3", "sample description", sampleDag, core.Dict{"1": "2"})
	// task4, _ := core.NewTask("SampleTask4", "sample description", sampleDag, core.Dict{"1": "2"})
	// task5, _ := core.NewTask("SampleTask5", "sample description", sampleDag, core.Dict{"1": "2"})
	// task6, _ := core.NewTask("SampleTask6", "sample description", sampleDag, core.Dict{"1": "2"})
	// task1.AddUpstream(task2)
	// task5.AddUpstream(task6)
	// // task6.AddUpstream(task4)
	// task4.AddUpstream(task5)
	// task4.AddUpstream(task1)
	// sampleDag.LogInfo()
	// for task := range sampleDag.TopologicalSortedTasks() {
	// 	fmt.Println(task.GetName())
	// }
	// task3.Void()

	if len(os.Args) < 2 {
		sugar.Fatalw("Failed to provide subcommand.")
	}

	commands := registerSubCommands()
	parseSubCommand(commands)
}

func registerSubCommands() []core.CommandInterface {
	// TODO: Allow users to add their own custom commands.
	// This can be done by GO plugins
	// https://medium.com/learning-the-go-programming-language/writing-modular-go-programs-with-plugins-ec46381ee1a9
	commands := make([]core.CommandInterface, 0)
	commands = append(commands,
		scheduler.NewCommand(),
		server.NewCommand(),
		worker.NewCommand())
	return commands
}

func parseSubCommand(commands []core.CommandInterface) {
	sugar := logger.GetSugaredLogger()
	success := false
	for _, command := range commands {
		if command.GetCommandString() == os.Args[1] {
			command.RunCommand()
			success = true
		}
	}
	if !success {
		sugar.Fatalw("Invalid subcommand.")
		flag.PrintDefaults()
		os.Exit(1)
	}
}
