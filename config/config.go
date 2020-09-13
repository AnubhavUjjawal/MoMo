package config

import (
	"os"
	"path"
	"strconv"
	"time"

	"github.com/AnubhavUjjawal/MoMo/logger"
)

const momoHome = "~/momo"

// DagFileExtension is the extension of Dag files. Basically, they are
// go file compiled as plugins.
const DagFileExtension = ".so"

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

// GetMoMoHome returns MoMo home directory path.
func GetMoMoHome() string {
	return getEnv("MOMO_HOME", momoHome)
}

// GetDagsDir returns Dags dir.
func GetDagsDir() string {
	return path.Join(GetMoMoHome(), "dags")
}

// GetDagsParseInterval returns DAGS_PARSE_INTERVAL used by scheduler.
func GetDagsParseInterval() time.Duration {
	intervalStr := getEnv("DAGS_PARSE_INTERVAL", "5s")
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		logger.GetSugaredLogger().Fatalw("Invalid DAGS_PARSE_INTERVAL", "DAGS_PARSE_INTERVAL", intervalStr)
	}
	return interval
}

// GetDagRunCheckInterval returns DAG_RUN_CHECK_INTERVAL used by scheduler. It is
// the time which scheduler must wait before checking which tasks need to be
// scheduled.
func GetDagRunCheckInterval() time.Duration {
	intervalStr := getEnv("DAG_RUN_CHECK_INTERVAL", "5s")
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		logger.GetSugaredLogger().Fatalw("Invalid DAG_RUN_CHECK_INTERVAL", "DAG_RUN_CHECK_INTERVAL", intervalStr)
	}
	return interval
}

// GetRuntimeEnv returns RUNTIME_ENV. Used to configuration purposes.
// Same method is present in logger package to avoid circular import.
func GetRuntimeEnv() string {
	return getEnv("RUNTIME_ENV", "DEV")
}

// NumCocurrencyGoRoutine returns NUM_CONCURRENCY_GO_ROUTINE, which tells us how many
// goroutines to use when we want to do parallel processing.
func NumCocurrencyGoRoutine() int {
	sugar := logger.GetSugaredLogger()
	num, err := strconv.Atoi(getEnv("NUM_CONCURRENCY_GO_ROUTINE", "10"))

	if err != nil || num < 1 {
		sugar.Fatalf("Invalid Value for NUM_CONCURRENCY_GO_ROUTINE %s",
			getEnv("NUM_CONCURRENCY_GO_ROUTINE", "10"))
	}
	return num
}

// GetDataStore returns the persistence backend type we are using.
func GetDataStore() string {
	return getEnv("DATASTORE", "REDIS")
}

// GetPubSubClient returns the pub sub backend type we are using.
func GetPubSubClient() string {
	return getEnv("PUB_SUB", "REDIS")
}

// GetNumWorkers tell us number of worker goroutines to spawn per worker
func GetNumWorkers() int {
	sugar := logger.GetSugaredLogger()
	num, err := strconv.Atoi(getEnv("NUM_WORKERS", "2"))

	if err != nil || num < 1 {
		sugar.Fatalf("Invalid Value for NUM_WORKERS %s",
			getEnv("NUM_WORKERS", "2"))
	}
	return num
}
