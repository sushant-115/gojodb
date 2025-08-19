// Package logger provides a standardized, high-performance logging setup
// for the GojoDB project, built on top of Zap.
package logger

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config holds all the configuration for the logger.
type Config struct {
	// Level sets the minimum log level (e.g., "debug", "info", "warn", "error").
	Level string `yaml:"level"`
	// Format specifies the log output format ("json" or "console").
	Format string `yaml:"format"`
	// OutputFile specifies the file to write logs to. "stdout" or "stderr"
	// can be used to log to the console.
	OutputFile string `yaml:"output_file"`
}

// New creates a new zap.Logger based on the provided configuration.
// It's designed to be called once at application startup.
func New(config Config) (*zap.Logger, error) {
	// Parse and set the log level. Defaults to "info".
	logLevel := zap.NewAtomicLevel()
	if err := logLevel.UnmarshalText([]byte(config.Level)); err != nil {
		logLevel.SetLevel(zap.InfoLevel)
	}

	// Configure the output writer (WriteSyncer).
	writeSyncer, err := getWriteSyncer(config.OutputFile)
	if err != nil {
		return nil, err
	}

	// Configure the encoder (how logs are formatted).
	encoder := getEncoder(config.Format)

	// Create the logger core which combines level, encoder, and writer.
	core := zapcore.NewCore(encoder, writeSyncer, logLevel)

	// Create the final logger, adding the initial "service" field.
	logger := zap.New(core, zap.AddCaller()).
		WithOptions(zap.Fields(zap.String("service", "gojodb")))

	return logger, nil
}

// getEncoder selects the log encoder based on the configured format.
func getEncoder(format string) zapcore.Encoder {
	// Use a production-ready encoder configuration.
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

	// Return a JSON encoder for production or a human-friendly console encoder.
	if strings.ToLower(format) == "console" {
		return zapcore.NewConsoleEncoder(encoderConfig)
	}
	return zapcore.NewJSONEncoder(encoderConfig)
}

// getWriteSyncer selects the output destination for the logs.
func getWriteSyncer(outputFile string) (zapcore.WriteSyncer, error) {
	switch strings.ToLower(outputFile) {
	case "stdout", "":
		return zapcore.AddSync(os.Stdout), nil
	case "stderr":
		return zapcore.AddSync(os.Stderr), nil
	default:
		// Append to the file if it exists, or create it.
		file, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", outputFile, err)
		}
		return zapcore.AddSync(file), nil
	}
}
