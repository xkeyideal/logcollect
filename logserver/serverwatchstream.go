package logserver

import (
	"fmt"
	"io"
	"path/filepath"
	"sync"

	"github.com/xkeyideal/logcollect/chunkfile"
	"github.com/xkeyideal/logcollect/config"
	pb "github.com/xkeyideal/logcollect/logpb"

	"go.uber.org/zap"
)

type serverWatchStream struct {
	logID      string
	lg         *zap.Logger
	owner      *LogServer
	grpcStream pb.LogRPC_CollectionLogServer

	writeSize   int64
	chunkWriter *chunkfile.ChunkFileWriter

	wg    sync.WaitGroup
	once  sync.Once
	exitc chan struct{}
}

func newServerWatchStream(logDir, logDate, logID string,
	lg *zap.Logger,
	stream pb.LogRPC_CollectionLogServer,
	owner *LogServer) (*serverWatchStream, error) {

	datapath := filepath.Join(logDir, logDate, logID)

	chunkWriter, err := chunkfile.NewChunkFileWriter(config.LogChunkFileName, datapath,
		logMaxBytesPerFile, logSyncEvery, logSyncTimeout)
	if err != nil {
		return nil, err
	}

	return &serverWatchStream{
		logID:      logID,
		lg:         lg,
		owner:      owner,
		grpcStream: stream,

		chunkWriter: chunkWriter,
		exitc:       make(chan struct{}),
	}, nil
}

func (sws *serverWatchStream) close() {
	sws.chunkWriter.Close()
	close(sws.exitc)
	sws.wg.Wait()
	sws.lg.Debug("write close", zap.String("log-id", sws.logID))
}

func (sws *serverWatchStream) recvloop() error {
	for {
		content, err := sws.grpcStream.Recv()
		if err == io.EOF {
			sws.grpcStream.SendAndClose(&pb.LogResponse{Size: sws.writeSize})
			return nil
		}

		if err != nil {
			return err
		}

		n := int64(len(content.Content))
		if n <= 0 {
			continue
		}

		err = sws.chunkWriter.Put(content.Content)
		if err != nil {
			fmt.Println("sws.chunkWriter.Put: ", err)
		}
		sws.writeSize += n
	}
}

func (sws *serverWatchStream) notifyReaders() {
	sws.wg.Add(1)

	for {
		select {
		case <-sws.chunkWriter.SyncChan():
			sws.owner.serverNotifyReaders(sws.logID)
		case <-sws.exitc:
			goto exit
		}
	}
exit:
	sws.wg.Done()
}
