package logserver

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xkeyideal/logcollect/chunkfile"
	pb "github.com/xkeyideal/logcollect/logpb"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	logIDKey = "log-id"
	logDate  = "log-date"

	// 每个log chunk file的大小
	logMaxBytesPerFile = 8 * 1024 * 1024 // 8MB

	// 每次read的缓冲区大小
	logReadMsgSize = 1024

	// log 刷盘的参数
	logSyncEvery   = 50
	logSyncTimeout = 5 * time.Second

	// 客户端查询logID日志时，改日志的服务端的工作状况
	logSeverDone     = 1
	logServerRunning = 2
	logServerRework  = 3
)

type LogServer struct {
	logDir string
	status int32

	pb.UnimplementedLogRPCServer

	lg *zap.Logger

	watchStore map[string][]*clientWatchStream
	wslock     sync.RWMutex

	runningServers map[string]*serverWatchStream
	rslock         sync.RWMutex
}

func NewLogServer(logDir string, lg *zap.Logger) *LogServer {
	return &LogServer{
		logDir:         logDir,
		lg:             lg,
		watchStore:     make(map[string][]*clientWatchStream),
		runningServers: make(map[string]*serverWatchStream),
	}
}

func (s *LogServer) CollectionLog(stream pb.LogRPC_CollectionLogServer) error {
	if atomic.LoadInt32(&s.status) == 1 {
		return status.Error(codes.Aborted, "logserver stopped")
	}

	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return status.Error(codes.InvalidArgument, "log-id & log-date metadata is required")
	}

	var (
		logID string
		date  string
	)

	if len(md.Get(logIDKey)) > 0 {
		logID = strings.TrimSpace(md.Get(logIDKey)[0])
	}
	if logID == "" {
		return status.Error(codes.InvalidArgument, "log-id metadata is required")
	}

	if len(md.Get(logDate)) > 0 {
		date = strings.TrimSpace(md.Get(logDate)[0])
	}
	if date == "" {
		return status.Error(codes.InvalidArgument, "log-date metadata is required")
	}

	if s.runningServer(logID) {
		return status.Error(codes.AlreadyExists, fmt.Sprintf("[%s] logId 服务已经在运行，请保证logId的全局唯一", logID))
	}

	sws, err := newServerWatchStream(s.logDir, date, logID, s.lg, stream, s)
	if err != nil {
		return status.Error(codes.DataLoss, err.Error())
	}

	s.lg.Debug("recvlog", zap.String("date", date), zap.String("log-id", logID))

	s.addServer(logID, sws)

	var err2 error
	errc := make(chan error, 1)
	go func() {
		rerr := sws.recvloop()
		if rerr != nil {
			s.lg.Warn("recvloop", zap.String("log-id", logID), zap.Error(rerr))
		}
		errc <- rerr
	}()

	go sws.notifyReaders()

	s.storeLogQueryServerStatus(logID, logServerRunning)

	select {
	case err2 = <-errc:
	case <-stream.Context().Done():
		err = stream.Context().Err()
	}

	s.delServer(logID)
	sws.once.Do(sws.close)
	s.storeLogQueryServerStatus(logID, logSeverDone)

	return err2
}

func (s *LogServer) QueryLog(req *pb.LogQuery, stream pb.LogRPC_QueryLogServer) error {
	if atomic.LoadInt32(&s.status) == 1 {
		return status.Error(codes.Aborted, "logserver stopped")
	}

	logID := req.LogId
	if logID == "" {
		return status.Error(codes.InvalidArgument, "log-id metadata is required")
	}

	var (
		logServerStatus int32 = logSeverDone
	)

	ok := s.runningServer(logID)
	if ok {
		logServerStatus = logServerRunning
	}

	// 本地文件不存在，尝试从远端存储下载
	if !chunkfile.FileOrDirExist(filepath.Join(s.logDir, req.Date, logID)) {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("[%s/%s]该日志不存在", req.Date, logID))
	}

	cws, err := newClientWatchStream(s.logDir, logID, logServerStatus, s.lg, req, stream)
	if err != nil {
		return status.Error(codes.DataLoss, err.Error())
	}

	s.lg.Debug("querylog", zap.String("date", req.Date), zap.String("log-id", logID),
		zap.Int64("seek", req.Seek), zap.Int32("server", logServerStatus))

	s.addLogQuery(logID, cws)

	donec := make(chan bool, 1)
	go func() {
		done := cws.readerLoop()
		if done {
			s.lg.Info("reader chunk file done", zap.String("uuid", cws.uuid), zap.String("date", req.Date), zap.String("log-id", logID))
		}
		donec <- done
	}()

	errc := make(chan error, 1)
	go func() {
		serr := cws.sendloop()
		if serr != nil {
			s.lg.Warn("failed to send reader response to gRPC stream", zap.String("uuid", cws.uuid), zap.Error(serr))
		}
		errc <- serr
	}()

	select {
	case err = <-errc:
	case <-donec:
		err = nil
	case <-stream.Context().Done():
		err = stream.Context().Err()
	}

	s.delLogQuery(logID, cws.uuid)
	cws.once.Do(cws.close)

	return err
}

func (s *LogServer) Stop() {
	atomic.StoreInt32(&s.status, 1)

	s.rslock.Lock()
	for _, server := range s.runningServers {
		server.once.Do(server.close)
		server = nil
	}
	s.runningServers = make(map[string]*serverWatchStream)
	s.rslock.Unlock()

	s.wslock.Lock()
	for _, watchers := range s.watchStore {
		for _, watcher := range watchers {
			watcher.once.Do(watcher.close)
			watcher = nil
		}
	}
	s.watchStore = make(map[string][]*clientWatchStream)
	s.wslock.Unlock()
}

func (s *LogServer) serverNotifyReaders(logID string) {
	s.wslock.RLock()
	watchers := make([]*clientWatchStream, len(s.watchStore[logID]))
	copy(watchers, s.watchStore[logID])
	s.wslock.RUnlock()

	for _, watcher := range watchers {
		watcher.chunkReader.ReloadWriteParamters()
	}
}

func (s *LogServer) storeLogQueryServerStatus(logID string, status int32) {
	s.wslock.RLock()
	watchers := make([]*clientWatchStream, len(s.watchStore[logID]))
	copy(watchers, s.watchStore[logID])
	s.wslock.RUnlock()

	for _, watcher := range watchers {
		watcher.storeServerStatus(status)
	}
}

func (s *LogServer) addLogQuery(logID string, cws *clientWatchStream) {
	s.wslock.Lock()
	_, ok := s.watchStore[logID]
	if ok {
		s.watchStore[logID] = append(s.watchStore[logID], cws)
	} else {
		s.watchStore[logID] = []*clientWatchStream{cws}
	}

	s.wslock.Unlock()
}

func (s *LogServer) delLogQuery(logID string, uuid string) {
	s.wslock.Lock()
	watchers := s.watchStore[logID]
	index := -1
	for i, watcher := range watchers {
		if watcher.uuid == uuid {
			index = i
			break
		}
	}

	if index >= 0 {
		if len(watchers) == 1 {
			delete(s.watchStore, logID)
		} else {
			watchers[index], watchers[len(watchers)-1] = watchers[len(watchers)-1], watchers[index]
			watchers = watchers[:len(watchers)-1]
			s.watchStore[logID] = watchers
		}
	}

	s.wslock.Unlock()
}

func (s *LogServer) addServer(logID string, sws *serverWatchStream) {
	s.rslock.Lock()
	s.runningServers[logID] = sws
	s.rslock.Unlock()
}

func (s *LogServer) delServer(logID string) {
	s.rslock.Lock()
	delete(s.runningServers, logID)
	s.rslock.Unlock()
}

func (s *LogServer) runningServer(logID string) bool {
	s.rslock.RLock()
	_, ok := s.runningServers[logID]
	s.rslock.RUnlock()

	return ok
}
