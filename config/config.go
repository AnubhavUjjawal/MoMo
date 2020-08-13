package config

import (
	"os"
	"path"
	"strconv"

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
func GetDagsParseInterval() string {
	return getEnv("DAGS_PARSE_INTERVAL", "5s")
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
			getEnv("NUM_CONCURRENCY_GO_ROUTINE", "5"))
	}
	return num
}
