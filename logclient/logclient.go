package logclient

import (
	"context"
	"fmt"
	"io"
	"log"

	pb "github.com/xkeyideal/logcollect/logpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	logIDKey = "log-id"
	logDate  = "log-date"
)

type LogClient struct {
	remote pb.LogRPCClient
}

func NewLogClient(conn *grpc.ClientConn) *LogClient {
	return &LogClient{
		remote: pb.NewLogRPCClient(conn),
	}
}

func (lc *LogClient) CollectLog(ctx context.Context, logID string,
	datac <-chan []byte, donec chan struct{}) {

	md := metadata.Pairs(logIDKey, logID, logDate, "2020-04-02")
	ctx = metadata.NewOutgoingContext(ctx, md)

	stream, err := lc.remote.CollectionLog(ctx)
	if err != nil {
		log.Fatalf("%v.CollectionLog, %v", lc.remote, err)
	}

xxx:
	for {
		select {
		case data := <-datac:
			content := &pb.LogContent{
				Content: data,
			}
			err := stream.Send(content)
			if err == io.EOF {
				fmt.Println("io.EOF")
				break xxx
			}

			if err != nil {
				log.Fatalf("CollectionLog Send, %v", err)
			}
		case <-donec:
			break xxx
		}
	}

	reply, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("%v.CloseAndRecv() got error %v", stream, err)
	}

	log.Printf("CollectLog summary: %v", reply)
}

func (lc *LogClient) QueryLog(logID string, seek int64) {
	in := &pb.LogQuery{
		LogId: logID,
		Date:  "2020-04-02",
		Seek:  seek,
	}

	stream, err := lc.remote.QueryLog(context.Background(), in)
	if err != nil {
		log.Fatalf("%v.QueryLog, %v", lc.remote, err)
	}

	for {
		content, err := stream.Recv()
		if err == io.EOF {
			//log.Print("io.EOF")
			break
		}

		if err != nil {
			log.Fatalf("QueryLog Recv, %v", err)
		}

		//log.Print(string(content.Content))
		fmt.Print(string(content.Content))
	}

	fmt.Println()
}
