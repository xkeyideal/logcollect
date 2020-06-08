package chunkfile

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync/atomic"
	"time"
)

var ErrWriterExited = errors.New("chunk writer exited")

type ChunkFileWriter struct {
	// run-time state (also persisted to disk)
	writePos     int64
	writeFileNum int64

	exitFlag int32
	name     string
	dataPath string

	// 每个chunk文件的大小，常量值，确定后不能修改
	maxBytesPerFile int64

	// 写入的数据条数，触发刷盘
	syncEvery int64

	// 刷盘间隔时间
	syncTimeout time.Duration
	needSync    bool

	writeFile *os.File

	// 接收应用程序写入数据
	writeChan chan []byte

	// 控制同步写
	writeResponseChan chan error

	// 刷盘成功后的信号，用于通知chunkreader重新读取metadata
	// 与chunk reader的forceTailChan channel同理，需要有缓冲
	// 并且要求应用程序必须消费
	syncChan chan struct{}

	// chunk writer 接收退出信号
	exitChan     chan struct{}
	exitSyncChan chan struct{}
}

func NewChunkFileWriter(name, datapath string,
	maxBytesPerFile, syncEvery int64,
	syncTimeout time.Duration) (*ChunkFileWriter, error) {
	cfw := &ChunkFileWriter{
		name:              name,
		dataPath:          datapath,
		maxBytesPerFile:   maxBytesPerFile,
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		writeChan:         make(chan []byte, 16),
		writeResponseChan: make(chan error),
		syncChan:          make(chan struct{}, 1),
		exitChan:          make(chan struct{}),
		exitSyncChan:      make(chan struct{}),
	}

	var err error

	cfw.writeFileNum, cfw.writePos, err = retrieveMetaData(name, datapath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if !FileOrDirExist(datapath) {
		err = mkdir(datapath)
		if err != nil {
			return nil, err
		}
	}

	//fmt.Println("metadata: ", cfw.writeFileNum, cfw.writePos)

	go cfw.ioLoop()

	return cfw, nil
}

func (cfw *ChunkFileWriter) GetMetadata() (int64, int64) {
	return cfw.writePos, cfw.writeFileNum
}

func (cfw *ChunkFileWriter) SyncChan() <-chan struct{} {
	return cfw.syncChan
}

// Put 写入数据
func (cfw *ChunkFileWriter) Put(data []byte) error {
	if atomic.LoadInt32(&cfw.exitFlag) == 1 {
		return ErrWriterExited
	}

	cfw.writeChan <- data

	return <-cfw.writeResponseChan
}

func (cfw *ChunkFileWriter) Close() error {
	cfw.exit()
	return cfw.sync()
}

func (cfw *ChunkFileWriter) exit() {
	atomic.StoreInt32(&cfw.exitFlag, 1)

	close(cfw.exitChan)
	// ensure that ioLoop has exited
	<-cfw.exitSyncChan
}

func (cfw *ChunkFileWriter) persistMetadata() error {
	var f *os.File
	var err error

	fileName := metaDataFileName(cfw.name, cfw.dataPath)
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d,%d\n", cfw.writeFileNum, cfw.writePos)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (cfw *ChunkFileWriter) sync() error {
	if cfw.writeFile != nil {
		cfw.writeFile.Sync()
		cfw.writeFile.Close()
		cfw.writeFile = nil
	}

	err := cfw.persistMetadata()
	if err != nil {
		return err
	}

	cfw.needSync = false
	return nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (cfw *ChunkFileWriter) writeOne(data []byte) error {
	var (
		err error
		n   int
	)

	if cfw.writeFile == nil {
		curFileName := fileName(cfw.name, cfw.dataPath, cfw.writeFileNum)
		cfw.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		if cfw.writePos > 0 {
			_, err = cfw.writeFile.Seek(cfw.writePos, 0)
			if err != nil {
				cfw.writeFile.Close()
				cfw.writeFile = nil
				return err
			}
		}
	}

	n, err = cfw.writeFile.Write(data)
	if err != nil {
		cfw.writeFile.Close()
		cfw.writeFile = nil
		return err
	}

	cfw.writePos += int64(n)

	// 超过每个文件的最大字节数，需要重开文件写入
	if cfw.writePos >= cfw.maxBytesPerFile {
		cfw.writeFileNum++
		cfw.writePos = 0

		// sync every time we start writing to a new file
		err = cfw.sync()
		if cfw.writeFile != nil {
			cfw.writeFile.Close()
			cfw.writeFile = nil
		}
	}

	return err
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (cfw *ChunkFileWriter) ioLoop() {
	var err error
	var count int64

	syncTicker := time.NewTicker(cfw.syncTimeout)

	for {
		// dont sync all the time :)
		if count == cfw.syncEvery {
			cfw.needSync = true
		}

		if cfw.needSync {
			err = cfw.sync()
			if err != nil {
				//d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			} else {
				count = 0
				select {
				case cfw.syncChan <- struct{}{}:
				case <-cfw.exitChan:
					goto exit
				}
			}
		}

		select {
		// 写入数据
		case dataWrite := <-cfw.writeChan:
			count++
			cfw.writeResponseChan <- cfw.writeOne(dataWrite)
		// 定时刷盘
		case <-syncTicker.C:
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			cfw.needSync = true
		case <-cfw.exitChan:
			goto exit
		}
	}

exit:
	syncTicker.Stop()
	cfw.exitSyncChan <- struct{}{}
}
