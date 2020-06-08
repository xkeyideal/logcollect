package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

func NewLogger(logFilename string, level zapcore.Level, stdout bool) *zap.Logger {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder, // 小写编码器
		EncodeTime:     zapcore.ISO8601TimeEncoder,  // ISO8601 UTC 时间格式
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.FullCallerEncoder, // 全路径编码器
	}

	hook := lumberjack.Logger{
		Filename:   logFilename, // 日志文件路径
		MaxSize:    512,         // 每个日志文件保存的最大尺寸 单位：M
		MaxBackups: 300,         // 日志文件最多保存多少个备份
		MaxAge:     7,           // 文件最多保存多少天
		Compress:   true,        // 是否压缩
	}

	// 设置日志级别
	atomicLevel := zap.NewAtomicLevel()
	atomicLevel.SetLevel(level)

	writeSyncer := []zapcore.WriteSyncer{zapcore.AddSync(&hook)}
	if stdout {
		writeSyncer = append(writeSyncer, zapcore.Lock(os.Stdout))
	}

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),    // 编码器配置
		zapcore.NewMultiWriteSyncer(writeSyncer...), // 打印到控制台和文件
		atomicLevel, // 日志级别
	)

	logger := zap.New(core)

	return logger
}
