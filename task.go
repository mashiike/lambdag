package lambdag

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

type Task struct {
	dag     *DAG
	id      string
	handler TaskHandler
	opts    TaskOptions
}

type TaskOptions struct {
}

type TaskRequest struct {
	DAGRunID      string
	DAGRunConfig  json.RawMessage
	TaskResponses map[string]json.RawMessage
	Logger        *log.Logger
}

type TaskHandler interface {
	Invoke(context.Context, *TaskRequest) (interface{}, error)
}

type TaskHandlerFunc func(context.Context, *TaskRequest) (interface{}, error)

func (h TaskHandlerFunc) Invoke(ctx context.Context, req *TaskRequest) (interface{}, error) {
	return h(ctx, req)
}

func newTask(dag *DAG, id string, handler TaskHandler, optFns ...func(opts *TaskOptions) error) (*Task, error) {
	task := &Task{
		dag:     dag,
		id:      id,
		handler: handler,
	}
	for _, optFn := range optFns {
		if err := optFn(&task.opts); err != nil {
			return nil, err
		}
	}
	return task, nil
}

func (task *Task) ID() string {
	return task.id
}

func (task *Task) TaskHandler() TaskHandler {
	return task.handler
}

func (task *Task) SetDownstream(descendants ...*Task) error {
	for _, descendant := range descendants {
		if err := task.dag.AddDependency(task, descendant); err != nil {
			return err
		}
	}
	return nil
}

func (task *Task) SetUpstream(ancestors ...*Task) error {
	for _, ancestor := range ancestors {
		if err := task.dag.AddDependency(ancestor, task); err != nil {
			return err
		}
	}
	return nil
}

func (task *Task) String() string {
	return task.id
}

func (task *Task) GoString() string {
	return fmt.Sprintf("*lambdag.Task{ID:%s}", task.ID())
}
