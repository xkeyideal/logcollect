package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/xkeyideal/logcollect/engine"
)

func main() {
	pidFile := "/run/runner.pid"

	engine, err := engine.NewGrpcEngine()
	if err != nil {
		log.Fatal(err)
	}

	pid := os.Getpid()
	wf, err := os.OpenFile(pidFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Fatal(err)
	}
	_, err = fmt.Fprintf(wf, "%d\n", pid)
	wf.Sync()
	wf.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)

	fmt.Println(<-signals)

	engine.Stop()
}
