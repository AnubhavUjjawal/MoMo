package config

import (
	"os"
	"path"
)

const momoHome = "~/momo"

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

// GetDagsDir returns Dags dir
func GetDagsDir() string {
	return path.Join(GetMoMoHome(), "dags")
}
