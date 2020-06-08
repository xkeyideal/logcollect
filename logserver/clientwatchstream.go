package logserver

import (
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/xkeyideal/logcollect/chunkfile"
	"github.com/xkeyideal/logcollect/config"
	pb "github.com/xkeyideal/logcollect/logpb"

	uuid "github.com/satori/go.uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type clientWatchStream struct {
	uuid string

	lg     *zap.Logger
	req    *pb.LogQuery
	stream pb.LogRPC_QueryLogServer

	logServerStatus int32
	readPos         int64
	writePos        int64
	writeFileNum    int64

	chunkReader *chunkfile.ChunkFileReader

	wg   sync.WaitGroup
	once sync.Once

	// readTailChan,forceTailChan 必须为有缓冲channel
	// 在chunkread.go的ioLoop函数中，可能多次写入readTailChan,forceTailChan，
	// readerLoop,sendloop方法收到readTailChan,forceTailChan数据后直接退出，
	// readTailChan,forceTailChan的后续数据不会再有消费，若为无缓冲channel，会在ioLoop中造成死锁
	readTailChan  chan struct{}
	forceTailChan chan error

	messagec chan *pb.QueryResponse
	exitc    chan struct{}
}

func newClientWatchStream(logDir, logID string,
	logServerStatus int32, lg *zap.Logger,
	req *pb.LogQuery, stream pb.LogRPC_QueryLogServer) (*clientWatchStream, error) {

	datapath := filepath.Join(logDir, req.Date, logID)
	cws := &clientWatchStream{
		uuid:            uuid.NewV4().String(),
		lg:              lg,
		req:             req,
		stream:          stream,
		logServerStatus: logServerStatus,
		readPos:         req.Seek,

		readTailChan:  make(chan struct{}, 1),
		forceTailChan: make(chan error, 1),
		messagec:      make(chan *pb.QueryResponse),
		exitc:         make(chan struct{}),
	}

	var typ int32 = chunkfile.FileReadOnly
	if logServerStatus == logServerRunning {
		typ = chunkfile.FileReadWrite
	}

	chunkReader, err := chunkfile.NewChunkFileReader(config.LogChunkFileName, datapath,
		req.Seek, logMaxBytesPerFile, logReadMsgSize, typ,
		cws.readTailChan, cws.forceTailChan)
	if err != nil {
		return nil, err
	}

	cws.chunkReader = chunkReader
	cws.writeFileNum, cws.writePos = chunkReader.GetWriteParamters()

	return cws, nil
}

func (cws *clientWatchStream) sendloop() error {
	cws.wg.Add(1)
	for {
		select {
		case resp := <-cws.messagec:
			serr := cws.stream.Send(resp)
			if serr != nil {
				cws.wg.Done()
				return serr
			}
		case readerErr := <-cws.forceTailChan:
			if readerErr != nil {
				cws.wg.Done()
				return status.Error(codes.DataLoss, readerErr.Error())
			}
		case <-cws.exitc:
			goto exit
		}
	}
exit:
	cws.wg.Done()
	return nil
}

func (cws *clientWatchStream) readerLoop() bool {
	cws.wg.Add(1)
	for {
		select {
		case content := <-cws.chunkReader.ReadChan():
			n := int64(len(content))
			cws.readPos += n
			remainBytes := (cws.writeFileNum-1)*logMaxBytesPerFile + cws.writePos - cws.readPos
			if remainBytes < 0 {
				remainBytes = 0
			}

			cws.messagec <- &pb.QueryResponse{
				Content:     content,
				ReadPos:     cws.readPos,
				RemainBytes: remainBytes,
			}
		case <-cws.readTailChan:
			cws.lg.Debug("read tail", zap.String("uuid", cws.uuid), zap.String("log-id", cws.req.LogId))
			cws.wg.Done()
			return true
		case <-cws.exitc:
			goto exit
		}
	}
exit:
	cws.wg.Done()
	return false
}

func (cws *clientWatchStream) close() {
	cws.chunkReader.Close()
	close(cws.exitc)
	cws.wg.Wait()

	// 保证最后剩余的数据能够发送给客户端
	for content := range cws.chunkReader.ReadChan() {
		n := int64(len(content))
		cws.readPos += n
		remainBytes := (cws.writeFileNum-1)*logMaxBytesPerFile + cws.writePos - cws.readPos
		if remainBytes < 0 {
			remainBytes = 0
		}

		cws.stream.Send(&pb.QueryResponse{
			Content:     content,
			ReadPos:     cws.readPos,
			RemainBytes: remainBytes,
		})
	}

	cws.lg.Debug("read close", zap.String("uuid", cws.uuid), zap.String("log-id", cws.req.LogId))
}

func (cws *clientWatchStream) storeServerStatus(status int32) {
	atomic.StoreInt32(&cws.logServerStatus, status)
	var typ int32 = chunkfile.FileReadOnly
	if status == logServerRunning {
		typ = chunkfile.FileReadWrite
	}
	cws.chunkReader.StoreReaderType(typ)
}
