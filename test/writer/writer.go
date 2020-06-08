package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/xkeyideal/logcollect/logclient"

	"google.golang.org/grpc"
)

func main() {
	filename := "/tmp/dataerr.log"
	readFile, err := os.OpenFile(filename, os.O_RDONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer readFile.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := grpc.DialContext(ctx, "127.0.0.1:7809", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer cancel()
	defer conn.Close()

	lc := logclient.NewLogClient(conn)

	logID := "6e91b7ac-0afd-4688-8a2e-d1fcfc802091" //uuid.NewV4().String()
	fmt.Println(logID)

	datac := make(chan []byte)
	donec := make(chan struct{})

	go func() {
		reader := bufio.NewReader(readFile)
		for {
			readBuf := make([]byte, 32)
			n, err := reader.Read(readBuf)
			if err == io.EOF {
				donec <- struct{}{}
				log.Print(err, "xxx")
				break
			}

			if err != nil {
				log.Panic(err)
			}
			datac <- readBuf[:n]
			time.Sleep(3 * time.Second)
		}
	}()

	lc.CollectLog(context.Background(), logID, datac, donec)
}
