package server

import (
	"flag"

	"github.com/AnubhavUjjawal/MoMo/core"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type ServerCommand struct {
	core.Command
}

// RunCommand parses the flags starts the Server.
func (sch *ServerCommand) RunCommand(logger *zap.SugaredLogger) error {
	// TODO: parse flags before starting Server

	logger.Infow("Starting Server")
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
func NewCommand() *ServerCommand {
	commandString := "server"
	flagset := flag.NewFlagSet(commandString, flag.ExitOnError)
	return &ServerCommand{core.Command{CommandString: commandString, Flagset: flagset}}
}
