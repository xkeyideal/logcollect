package config

import (
	"math"

	"go.uber.org/zap/zapcore"
)

const (
	LogChunkFileName = "logcollect"
)

type ServerConfig struct {
	Port int
	// grpc相关配置项
	KeepAliveMinTime     int
	WriteBufferSize      int
	ReadBufferSize       int
	MaxRecvMsgSize       int
	MaxSendMsgSize       int
	MaxConcurrentStreams uint32

	// zap log相关配置项
	LogDir   string
	LogLevel zapcore.Level

	// log 存储位置
	LogStoreDir string
}

func NewServerConfig() *ServerConfig {
	cfg := &ServerConfig{
		Port:                 7809,
		KeepAliveMinTime:     10,
		WriteBufferSize:      20 * 1024 * 1024,
		ReadBufferSize:       20 * 1024 * 1024,
		MaxRecvMsgSize:       40 * 1024 * 1024,
		MaxSendMsgSize:       40 * 1024 * 1024,
		MaxConcurrentStreams: math.MaxUint32,

		LogDir:      "/tmp/logs/zaplog",
		LogLevel:    zapcore.DebugLevel,
		LogStoreDir: "/tmp/logs/log-collect",
	}

	return cfg
}
