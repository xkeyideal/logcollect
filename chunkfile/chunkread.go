package chunkfile

import (
	"bufio"
	"errors"
	"os"
	"sync"
	"sync/atomic"
)

var (
	ErrChunkFileEmpty   = errors.New("读取的文件为空")
	ErrReaderOutOfRange = errors.New("偏移位置超越文件大小")
)

// chunk 文件的读取状态，只读、边写边读
const (
	FileReadOnly  = 1
	FileReadWrite = 2
)

type ChunkFileReader struct {
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64

	sync.RWMutex

	name     string
	dataPath string

	// 每次读取的字节数
	perMsgBufferSize int64

	// 正在读取的文件的大小
	curReadFileSize int64

	// 每个文件理论上的最大值，由chunkwriter决定，一般为常量
	// 用于辅助判断是否需要切换文件reader，通常情况下使用 curReadFileSize
	maxBytesPerFile int64

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64
	nextReadFileNum int64

	typ int32

	readFile *os.File
	reader   *bufio.Reader

	readChan chan []byte

	// readTailChan,forceTailChan 必须为有缓冲channel
	// 在ioLoop函数中，可能多次写入readTailChan,forceTailChan，
	// 应用程序收到readTailChan,forceTailChan数据后直接退出，
	// readTailChan,forceTailChan的后续数据不会再有消费，若为无缓冲channel，会在ioLoop中造成死锁
	readTailChan  chan struct{}
	forceTailChan chan error

	// 边写边读时，用于接收重新加载meta.dat文件数据
	reloadChan chan struct{}

	// chunk reader 接收退出信号
	exitChan     chan struct{}
	exitSyncChan chan struct{}
}

func NewChunkFileReader(name, datapath string,
	readPos int64,
	maxBytesPerFile, perMsgBufferSize int64, typ int32,
	readTailChan chan struct{}, forceTailChan chan error) (*ChunkFileReader, error) {

	var (
		i            int64
		readFileNum  int64
		err          error
		writeFileNum int64
		writePos     int64
	)

	writeFileNum, writePos, err = retrieveMetaData(name, datapath)
	if err != nil {
		return nil, err
	}

	if writeFileNum <= 0 && writePos <= 0 {
		return nil, ErrChunkFileEmpty
	}

	for i = 0; i < writeFileNum; i++ {
		rfile, err := os.OpenFile(fileName(name, datapath, i), os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		stat, err := rfile.Stat()
		if err != nil {
			return nil, err
		}

		fileSize := stat.Size()
		rfile.Close()

		if fileSize > readPos {
			break
		}

		readPos = readPos - fileSize
		readFileNum = i + 1
	}

	// 读到最后一个文件时，才需要比较是否越界
	if readFileNum == writeFileNum && readPos >= writePos {
		return nil, ErrReaderOutOfRange
	}

	cfr := &ChunkFileReader{
		name:             name,
		dataPath:         datapath,
		maxBytesPerFile:  maxBytesPerFile,
		perMsgBufferSize: perMsgBufferSize,
		writeFileNum:     writeFileNum,
		writePos:         writePos,
		readFileNum:      readFileNum,
		readPos:          readPos,
		nextReadFileNum:  readFileNum,
		nextReadPos:      readPos,
		typ:              typ,
		readChan:         make(chan []byte),
		exitChan:         make(chan struct{}),
		exitSyncChan:     make(chan struct{}),
		readTailChan:     readTailChan,
		forceTailChan:    forceTailChan,
		reloadChan:       make(chan struct{}),
	}

	go cfr.ioLoop()

	return cfr, nil
}

func (cfr *ChunkFileReader) GetWriteParamters() (int64, int64) {
	cfr.RLock()
	defer cfr.RUnlock()

	return cfr.writeFileNum, cfr.writePos
}

func (cfr *ChunkFileReader) StoreReaderType(typ int32) {
	// 服务端边写边读的状态发生改变之前，reload一下
	cfr.reloadWriteParamters()
	atomic.StoreInt32(&cfr.typ, typ)
}

func (cfr *ChunkFileReader) ReloadWriteParamters() {
	cfr.reloadChan <- struct{}{}
}

func (cfr *ChunkFileReader) reloadWriteParamters() {
	writeFileNum, writePos, err := retrieveMetaData(cfr.name, cfr.dataPath)
	if err != nil {
		return
	}

	//fmt.Println("ReloadWriteParamters:", writeFileNum, writePos)

	cfr.Lock()
	cfr.writeFileNum = writeFileNum
	cfr.writePos = writePos

	// 当前读取的文件就是正在写入的文件，那么更新当前文件的size
	if cfr.readFileNum == writeFileNum {
		cfr.curReadFileSize = writePos
	} else if writeFileNum > cfr.readFileNum {
		// 当前正在读取的文件，写入时正好发生的切文件
		curFileName := fileName(cfr.name, cfr.dataPath, cfr.readFileNum)
		rfile, err := os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return
		}
		stat, err := rfile.Stat()
		if err != nil {
			return
		}
		cfr.curReadFileSize = stat.Size()
	}
	cfr.Unlock()
}

// ReadChan 读取数据
func (cfr *ChunkFileReader) ReadChan() <-chan []byte {
	return cfr.readChan
}

func (cfr *ChunkFileReader) Close() {
	cfr.exit()
}

func (cfr *ChunkFileReader) exit() {
	close(cfr.exitChan)
	<-cfr.exitSyncChan

	if cfr.readFile != nil {
		cfr.readFile.Close()
		cfr.readFile = nil
	}
	close(cfr.readChan)
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (cfr *ChunkFileReader) readOne(writeFileNum int64) ([]byte, error) {
	var err error

	if cfr.readFile == nil {
		curFileName := fileName(cfr.name, cfr.dataPath, cfr.readFileNum)
		cfr.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		if cfr.readPos > 0 {
			_, err = cfr.readFile.Seek(cfr.readPos, 0)
			if err != nil {
				cfr.readFile.Close()
				cfr.readFile = nil
				return nil, err
			}
		}

		stat, err := cfr.readFile.Stat()
		if err != nil {
			cfr.curReadFileSize = cfr.maxBytesPerFile
		} else {
			cfr.curReadFileSize = stat.Size()
		}

		cfr.reader = bufio.NewReader(cfr.readFile)
	}

	readBuf := make([]byte, cfr.perMsgBufferSize)
	n, err := cfr.reader.Read(readBuf)
	if err != nil {
		cfr.readFile.Close()
		cfr.readFile = nil
		return nil, err
	}

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	cfr.nextReadPos = cfr.readPos + int64(n)
	cfr.nextReadFileNum = cfr.readFileNum

	// 正在读取的文件，必须超过当前文件的最大size，才需要考虑是否换文件读
	if cfr.nextReadPos >= cfr.curReadFileSize {
		// 如果尚未读到当前chunk的最后一个文件，无论啥情况都应该切文件,继续读下一个文件
		if cfr.readFileNum < writeFileNum {
			if cfr.readFile != nil {
				cfr.readFile.Close()
				cfr.readFile = nil
			}

			cfr.nextReadFileNum++
			cfr.nextReadPos = 0
		} else {
			// 如果读到的是chunk的最后一个文件，只有在chunk文件是只读状态的时候才需要切文件（其实就是全部文件已经读完）
			// 边写边读状态时，此时需要等待写入
			if atomic.LoadInt32(&cfr.typ) == FileReadOnly {
				if cfr.readFile != nil {
					cfr.readFile.Close()
					cfr.readFile = nil
				}

				cfr.nextReadFileNum++
				cfr.nextReadPos = 0
			}
		}
	}

	return readBuf[:n], nil
}

func (cfr *ChunkFileReader) moveForward(writeFileNum, writePos int64) bool {
	cfr.readFileNum = cfr.nextReadFileNum
	cfr.readPos = cfr.nextReadPos

	//fmt.Println(cfr.readFileNum, cfr.readPos, cfr.curReadFileSize, writeFileNum, writePos)

	return cfr.checkTailCorruption(writeFileNum, writePos)
}

func (cfr *ChunkFileReader) checkTailCorruption(writeFileNum, writePos int64) bool {
	// 当前chunk文件的状态为边写边读状态，那么不应该退出
	if atomic.LoadInt32(&cfr.typ) == FileReadWrite {
		return false
	}

	// 否则判断文件是否读完，读完就结束

	// 当前读到的文件数目超过写入的文件数，说明最新的文件已经读完，读取应该结束
	if cfr.readFileNum > writeFileNum {
		return true
	}

	// 当前读取的文件与写入的文件数一致，但读取的字节数大于等于已写入的字节数，读取应该结束
	if cfr.readFileNum == writeFileNum && cfr.readPos >= writePos {
		return true
	}

	return false
}

func (cfr *ChunkFileReader) ioLoop() {
	var dataRead []byte
	var err error
	var r chan []byte

	for {
		cfr.RLock()
		writeFileNum := cfr.writeFileNum
		writePos := cfr.writePos
		cfr.RUnlock()

		readable := false
		if cfr.readFileNum < writeFileNum {
			readable = true
		} else if cfr.readFileNum == writeFileNum {
			if cfr.readPos < writePos {
				readable = true
			}
		}

		if readable {
			if cfr.nextReadPos == cfr.readPos {
				dataRead, err = cfr.readOne(writeFileNum)
				if err != nil {
					// 读文件出错，强制退出，避免死循环
					select {
					case cfr.forceTailChan <- err:
					case <-cfr.exitChan:
						goto exit
					}
				}
			}
			r = cfr.readChan
		} else {
			r = nil

			// 用于处理边写边读的情况下，写结束后，状态变为只读后此时文件已经读完的情况下退出
			// 需配合下面的default使用
			if atomic.LoadInt32(&cfr.typ) == FileReadOnly {
				select {
				case cfr.readTailChan <- struct{}{}:
				case <-cfr.exitChan:
					goto exit
				}
			}
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		case r <- dataRead:
			if cfr.moveForward(writeFileNum, writePos) {
				cfr.readTailChan <- struct{}{}
			}
		case <-cfr.reloadChan:
			cfr.reloadWriteParamters()
		case <-cfr.exitChan:
			goto exit
		default:
		}
	}

exit:
	cfr.exitSyncChan <- struct{}{}
}
