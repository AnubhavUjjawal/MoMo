package logger

import (
	"os"

	"go.uber.org/zap"
)

// Logger is the alias to zap.SugaredLogger
type Logger struct {
	*zap.SugaredLogger
}

var sugaredLogger *Logger

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// getRuntimeEnv returns RUNTIME_ENV. Used to configure logger, etc.
func getRuntimeEnv() string {
	return getEnv("RUNTIME_ENV", "DEV")
}

// GetSugaredLogger returns a singleton Logger based on `config.GetRuntimeEnv`
func GetSugaredLogger() *Logger {
	if sugaredLogger != nil {
		return sugaredLogger
	}
	env := getRuntimeEnv()
	var logger *zap.Logger
	switch env {
	case "DEV":
		logger, _ = zap.NewDevelopment()
		break
	default:
		logger, _ = zap.NewProduction()
	}

	defer logger.Sync() // flushes buffer, if any
	newLogger := Logger{logger.Sugar()}
	return &newLogger
}
