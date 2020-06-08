package main

import (
	"context"
	"log"
	"time"

	"github.com/xkeyideal/logcollect/logclient"

	"google.golang.org/grpc"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	conn, err := grpc.DialContext(ctx, "127.0.0.1:7809", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer cancel()
	defer conn.Close()

	lc := logclient.NewLogClient(conn)

	logID := "6e91b7ac-0afd-4688-8a2e-d1fcfc802091"

	lc.QueryLog(logID, 0)
}
