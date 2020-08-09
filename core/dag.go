package core

import (
	"time"

	"go.uber.org/zap"
)

// Dict type, can be used to hold arbitrary data.
type Dict map[interface{}]interface{}

// DAG is a basic collection of tasks that we want to run, organised in a way
// that specifies their dependencies and relationships.
type DAG struct {
	Name        string
	Schedule    time.Duration
	DefaultArgs Dict
	description string
}

// LogInfo logs DAG info using the passed logger.
func (dag *DAG) LogInfo(logger *zap.SugaredLogger) {
	logger.Infow("DAG info:", "DAG", dag.Name, "schedule", dag.Schedule)
}
