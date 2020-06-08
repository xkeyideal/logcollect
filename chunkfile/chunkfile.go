package chunkfile

import (
	"fmt"
	"os"
	"path"
)

func RetrieveMetaData(name, dataPath string) (int64, int64, error) {
	return retrieveMetaData(name, dataPath)
}

// retrieveMetaData initializes state from the filesystem
func retrieveMetaData(name, dataPath string) (int64, int64, error) {
	var f *os.File
	var err error

	fileName := metaDataFileName(name, dataPath)
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	var fileNum int64
	var writePos int64
	_, err = fmt.Fscanf(f, "%d,%d\n", &fileNum, &writePos)
	if err != nil {
		return 0, 0, err
	}

	return fileNum, writePos, nil
}

func MetaDataFileName(name, dataPath string) string {
	return metaDataFileName(name, dataPath)
}

func metaDataFileName(name, dataPath string) string {
	return fmt.Sprintf(path.Join(dataPath, "%s.meta.dat"), name)
}

func FileName(name, dataPath string, fileNum int64) string {
	return fileName(name, dataPath, fileNum)
}

func fileName(name, dataPath string, fileNum int64) string {
	return fmt.Sprintf(path.Join(dataPath, "%s.chunkfile.%03d.dat"), name, fileNum)
}

func FileOrDirExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func mkdir(dir string) error {
	return os.MkdirAll(dir, 0755)
}
