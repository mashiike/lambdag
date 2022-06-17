package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mashiike/lambdag"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	defer cancel()
	dag, err := lambdag.NewDAG("SampleDAG")
	if err != nil {
		log.Fatal(err)
	}
	task1, err := dag.NewTask("task1", lambdag.TaskHandlerFunc(func(ctx context.Context, tr *lambdag.TaskRequest) (*lambdag.TaskResponse, error) {
		return &lambdag.TaskResponse{
			Payload: json.RawMessage(`"task1 success"`),
		}, nil
	}))
	if err != nil {
		log.Fatal(err)
	}
	task2, err := dag.NewTask("task2", lambdag.TaskHandlerFunc(func(ctx context.Context, tr *lambdag.TaskRequest) (*lambdag.TaskResponse, error) {
		return &lambdag.TaskResponse{
			Payload: json.RawMessage("task2 success"),
		}, nil
	}))
	if err != nil {
		log.Fatal(err)
	}
	err = task1.SetDownstream(task2)
	if err != nil {
		log.Fatal(err)
	}
	lambdag.RunWithContext(ctx, os.Args[1:], dag)
}
