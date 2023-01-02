package logger

import (
	"os"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// @Author KHighness
// @Update 2022-12-28

// InitLogger initializes `zap.Logger` then we can use
// `zap.L()` or `zao.S()` to get global logger.
func InitLogger(level zapcore.Level) {
	var core zapcore.Core
	fileCore := zapcore.NewCore(zapFileEncoder(), zapWriteSyncer(), zapLevelEnabler())
	consoleCore := zapcore.NewCore(zapConsoleEncoder(), os.Stdout, level)
	core = zapcore.NewTee(fileCore, consoleCore)
	logger := zap.New(core, zap.AddCaller())
	zap.ReplaceGlobals(logger)
}

func zapLevelEnabler() zapcore.Level {
	return zapcore.InfoLevel
}

func zapEncodeConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		MessageKey:       "msg",
		LevelKey:         "level",
		TimeKey:          "ts",
		NameKey:          "logger",
		CallerKey:        "caller_line",
		FunctionKey:      zapcore.OmitKey,
		StacktraceKey:    "stacktrace",
		LineEnding:       "\n",
		EncodeLevel:      zapEncodeLevel,
		EncodeTime:       zapcore.ISO8601TimeEncoder,
		EncodeDuration:   zapcore.MillisDurationEncoder,
		EncodeCaller:     zapEncodeCaller,
		ConsoleSeparator: " ",
	}
}

func zapFileEncoder() zapcore.Encoder {
	return zapcore.NewJSONEncoder(zapEncodeConfig())
}

func zapConsoleEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(zapEncodeConfig())
}

func zapEncodeLevel(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString("[" + level.CapitalString() + "]")
}

func zapEncodeCaller(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(caller.TrimmedPath())
}

func zapWriteSyncer() zapcore.WriteSyncer {
	lumberJackLogger := &lumberjack.Logger{
		Filename:   "./log/khighdb.log",
		MaxSize:    1000,
		MaxBackups: 10,
		MaxAge:     30,
		Compress:   true,
	}
	return zapcore.AddSync(lumberJackLogger)
}
