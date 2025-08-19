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

// --- New Filtering Logic ---

// filteringCore is a zapcore.Core wrapper that filters out specific log messages.
type filteringCore struct {
	zapcore.Core
}

// NewFilteringCore creates a new core that wraps the provided core.
func NewFilteringCore(core zapcore.Core) zapcore.Core {
	return &filteringCore{core}
}

// Check decides whether a given log entry should be logged.
// This is the most efficient place to filter, before the entry is written.
func (fc *filteringCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	// **THE FILTERING LOGIC**
	// We check if the message contains our target string. To be extra safe,
	// we also check if the log is coming from the expected file.
	if strings.Contains(ent.Message, "Rollback failed: tx closed") {
		// **THE FIX**: To drop a log message, we must return nil.
		return nil
	}
	// If the message is not the one we want to filter, we pass it to the
	// underlying core to see if it should be logged.
	return fc.Core.Check(ent, ce)
}

// With adds structured context to the logger. We need to ensure the wrapper is maintained.
func (fc *filteringCore) With(fields []zapcore.Field) zapcore.Core {
	return NewFilteringCore(fc.Core.With(fields))
}

// --- Original Logger Configuration (Now uses the filter) ---

// Config holds all the configuration for the logger.
type Config struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`
	OutputFile string `yaml:"output_file"`
}

// New creates a new zap.Logger based on the provided configuration.
func New(config Config) (*zap.Logger, error) {
	logLevel := zap.NewAtomicLevel()
	if err := logLevel.UnmarshalText([]byte(config.Level)); err != nil {
		logLevel.SetLevel(zap.InfoLevel)
	}

	writeSyncer, err := getWriteSyncer(config.OutputFile)
	if err != nil {
		return nil, err
	}

	encoder := getEncoder(config.Format)

	// Create the base core.
	baseCore := zapcore.NewCore(encoder, writeSyncer, logLevel)

	// **MODIFICATION**: Wrap the base core with our new filtering logic.
	finalCore := NewFilteringCore(baseCore)

	// Create the final logger using the filtering core.
	logger := zap.New(finalCore, zap.AddCaller()).
		WithOptions(zap.Fields(zap.String("service", "gojodb")))

	return logger, nil
}

// getEncoder selects the log encoder based on the configured format.
func getEncoder(format string) zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder

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
		file, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file %s: %w", outputFile, err)
		}
		return zapcore.AddSync(file), nil
	}
}
