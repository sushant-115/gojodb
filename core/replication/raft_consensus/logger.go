// This file could be pkg/logger/raft_adapter.go

package fsm

import (
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/hashicorp/go-hclog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapRaftLogger is an adapter that allows a zap.Logger to be used
// as the logger for the HashiCorp Raft library.
type ZapRaftLogger struct {
	logger *zap.Logger
	name   string
	// level allows for dynamic control of the log level.
	level zap.AtomicLevel
}

// NewZapRaftLogger creates a new adapter.
// The zap logger should be a contextual logger for the raft component.
func NewZapRaftLogger(zapLogger *zap.Logger) *ZapRaftLogger {
	// We create an atomic level to allow dynamic level changes.
	// We'll attempt to set its initial level based on the passed-in logger's level.
	initialLevel := zap.InfoLevel // Default level
	if core := zapLogger.Core(); core.Enabled(zap.DebugLevel) {
		initialLevel = zap.DebugLevel
	}

	return &ZapRaftLogger{
		logger: zapLogger,
		level:  zap.NewAtomicLevelAt(initialLevel),
	}
}

// --- Interface methods required by hclog.Logger ---

func (z *ZapRaftLogger) Log(level hclog.Level, msg string, args ...interface{}) {
	// This is the generic log method, which we can ignore as Raft
	// primarily uses the leveled methods (Debug, Info, etc.).
}

func (z *ZapRaftLogger) Trace(msg string, args ...interface{}) {
	// hclog's Trace is extremely verbose, mapping to Zap's Debug.
	z.log(zap.DebugLevel, msg, args...)
}

func (z *ZapRaftLogger) Debug(msg string, args ...interface{}) {
	z.log(zap.DebugLevel, msg, args...)
}

func (z *ZapRaftLogger) Info(msg string, args ...interface{}) {
	z.log(zap.InfoLevel, msg, args...)
}

func (z *ZapRaftLogger) Warn(msg string, args ...interface{}) {
	z.log(zap.WarnLevel, msg, args...)
}

func (z *ZapRaftLogger) Error(msg string, args ...interface{}) {
	z.log(zap.ErrorLevel, msg, args...)
}

// --- Helper to perform the actual logging and filtering ---

func (z *ZapRaftLogger) log(level zapcore.Level, msg string, args ...interface{}) {
	// **THIS IS THE KEY**: Filter out the noisy "tx closed" message.
	if strings.Contains(msg, "tx closed") {
		return
	}

	// Check if the message should be logged based on the dynamic level.
	if !z.level.Enabled(level) {
		return
	}

	// Convert hclog's key-value args into Zap's structured fields.
	fields := z.argsToZapFields(args...)

	// Write the log entry using the underlying zap logger.
	if ce := z.logger.Check(level, msg); ce != nil {
		ce.Write(fields...)
	}
}

// --- Interface methods for creating sub-loggers ---

func (z *ZapRaftLogger) IsTrace() bool { return z.level.Enabled(zap.DebugLevel) }
func (z *ZapRaftLogger) IsDebug() bool { return z.level.Enabled(zap.DebugLevel) }
func (z *ZapRaftLogger) IsInfo() bool  { return z.level.Enabled(zap.InfoLevel) }
func (z *ZapRaftLogger) IsWarn() bool  { return z.level.Enabled(zap.WarnLevel) }
func (z *ZapRaftLogger) IsError() bool { return z.level.Enabled(zap.ErrorLevel) }

func (z *ZapRaftLogger) With(args ...interface{}) hclog.Logger {
	fields := z.argsToZapFields(args...)
	return &ZapRaftLogger{
		logger: z.logger.With(fields...),
		name:   z.name,
		level:  z.level, // Share the same atomic level
	}
}

func (z *ZapRaftLogger) Named(name string) hclog.Logger {
	newName := name
	if z.name != "" {
		newName = z.name + "." + name
	}
	return &ZapRaftLogger{
		logger: z.logger.Named(name),
		name:   newName,
		level:  z.level, // Share the same atomic level
	}
}

func (z *ZapRaftLogger) ResetNamed(name string) hclog.Logger {
	return &ZapRaftLogger{
		logger: z.logger.Named(name),
		name:   name,
		level:  z.level, // Share the same atomic level
	}
}

// --- New methods to satisfy the hclog.Logger interface ---

// GetLevel returns the current logging level.
func (z *ZapRaftLogger) GetLevel() hclog.Level {
	switch z.level.Level() {
	case zapcore.DebugLevel:
		return hclog.Debug
	case zapcore.InfoLevel:
		return hclog.Info
	case zapcore.WarnLevel:
		return hclog.Warn
	case zapcore.ErrorLevel:
		return hclog.Error
	default:
		return hclog.NoLevel
	}
}

// SetLevel changes the logging level.
func (z *ZapRaftLogger) SetLevel(level hclog.Level) {
	var zapLevel zapcore.Level
	switch level {
	case hclog.Trace, hclog.Debug:
		zapLevel = zap.DebugLevel
	case hclog.Info:
		zapLevel = zap.InfoLevel
	case hclog.Warn:
		zapLevel = zap.WarnLevel
	case hclog.Error:
		zapLevel = zap.ErrorLevel
	default:
		// hclog.NoLevel or Off, we can treat it as info or error.
		// Let's default to Info.
		zapLevel = zap.InfoLevel
	}
	z.level.SetLevel(zapLevel)
}

// --- Unchanged methods ---

// ImpliedArgs returns the arguments that are implied by the logger's context.
func (z *ZapRaftLogger) ImpliedArgs() []interface{} {
	// This is complex to implement with Zap and not strictly necessary
	// for Raft's operation. Returning nil is safe.
	return nil
}

// Name returns the name of the logger
func (z *ZapRaftLogger) Name() string {
	return z.name
}

// StandardLogger returns a standard logger that writes to this logger
func (z *ZapRaftLogger) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	// Implement if you need to pass a standard logger to a legacy library
	return nil
}

// StandardWriter returns a writer that writes to this logger
func (z *ZapRaftLogger) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	// Implement if you need to pass a writer to a legacy library
	return nil
}

// --- Utility to convert arguments ---

func (z *ZapRaftLogger) argsToZapFields(args ...interface{}) []zap.Field {
	fields := make([]zap.Field, 0, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			// Handle case where key is not a string, though hclog usually ensures this.
			key = fmt.Sprintf("invalid_key_%d", i)
		}
		if i+1 >= len(args) {
			fields = append(fields, zap.Any(key, "(no value)"))
			break
		}
		val := args[i+1]
		fields = append(fields, zap.Any(key, val))
	}
	return fields
}
