package engine

import (
	"fmt"
	"net"
	"path/filepath"
	"time"

	"github.com/xkeyideal/logcollect/config"
	pb "github.com/xkeyideal/logcollect/logpb"
	"github.com/xkeyideal/logcollect/logserver"

	"log"

	"github.com/xkeyideal/logcollect/logger"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type GrpcEngine struct {
	lg        *zap.Logger
	server    *grpc.Server
	logServer *logserver.LogServer
}

func NewGrpcEngine() (*GrpcEngine, error) {
	cfg := config.NewServerConfig()

	gopts := []grpc.ServerOption{}

	var kaep = keepalive.EnforcementPolicy{
		MinTime:             time.Duration(cfg.KeepAliveMinTime) * time.Second,
		PermitWithoutStream: true,
	}

	var kasp = keepalive.ServerParameters{
		Time:    2 * time.Hour,
		Timeout: 20 * time.Second,
	}

	gopts = append(gopts, grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))

	// 单位都是Byte
	gopts = append(gopts, grpc.WriteBufferSize(cfg.WriteBufferSize))
	gopts = append(gopts, grpc.ReadBufferSize(cfg.ReadBufferSize))
	gopts = append(gopts, grpc.MaxRecvMsgSize(cfg.MaxRecvMsgSize))
	gopts = append(gopts, grpc.MaxSendMsgSize(cfg.MaxSendMsgSize))
	gopts = append(gopts, grpc.MaxConcurrentStreams(cfg.MaxConcurrentStreams))

	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", cfg.Port))
	if err != nil {
		return nil, err
	}

	lgName := filepath.Join(cfg.LogDir, "runner.log")
	lg := logger.NewLogger(lgName, cfg.LogLevel, true)

	server := grpc.NewServer(gopts...)
	logServer := logserver.NewLogServer(cfg.LogStoreDir, lg)
	pb.RegisterLogRPCServer(server, logServer)

	go func() {
		err := server.Serve(listener)
		if err != nil {
			log.Fatal(err)
		}
	}()

	log.Printf("log server running port: %d", cfg.Port)

	return &GrpcEngine{
		lg:        lg,
		logServer: logServer,
		server:    server,
	}, nil
}

// Stop 下述顺序不能乱
func (ge *GrpcEngine) Stop() {
	ge.logServer.Stop()
	ge.server.Stop()
	ge.lg.Sync()
}
