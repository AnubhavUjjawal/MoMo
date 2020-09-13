package server

import (
	"flag"

	"github.com/AnubhavUjjawal/MoMo/logger"

	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/gin-gonic/gin"
)

type serverCommand struct {
	core.Command
}

// RunCommand parses the flags starts the Server.
func (sch *serverCommand) RunCommand() error {
	// TODO: parse flags before starting Server
	sugar := logger.GetSugaredLogger()
	sugar.Infow("Starting Server")
	r := gin.Default()
	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"message": "healthy",
		})
	})
	r.Run()
	return nil
}

// NewCommand creates and returns an instance of ServerCommand.
func NewCommand() *serverCommand {
	commandString := "server"
	flagset := flag.NewFlagSet(commandString, flag.ExitOnError)
	return &serverCommand{core.Command{CommandString: commandString, Flagset: flagset}}
}
