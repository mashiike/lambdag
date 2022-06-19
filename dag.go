package lambdag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"

	"github.com/Songmu/flextime"
	"github.com/aws/aws-lambda-go/lambda/messages"
	libdag "github.com/heimdalr/dag"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

type DAG struct {
	id           string
	opts         DAGOptions
	dependencies *libdag.DAG
}

type DAGOptions struct {
	newLoggerFunc            func(*DAGRunContext) (*log.Logger, error)
	numOfTasksInSingleInvoke int
	circuitBreaker           int
}

func WithDAGLogger(fn func(*DAGRunContext) (*log.Logger, error)) func(opts *DAGOptions) error {
	return func(opts *DAGOptions) error {
		opts.newLoggerFunc = fn
		return nil
	}
}

func WithNumOfTasksInSingleInvoke(num int) func(opts *DAGOptions) error {
	return func(opts *DAGOptions) error {
		opts.numOfTasksInSingleInvoke = num
		return nil
	}
}

func WithCircuitBreaker(num int) func(opts *DAGOptions) error {
	return func(opts *DAGOptions) error {
		opts.circuitBreaker = num
		return nil
	}
}

func NewDAG(id string, optFns ...func(opts *DAGOptions) error) (*DAG, error) {
	dag := &DAG{
		id:           id,
		dependencies: libdag.NewDAG(),
	}
	for _, optFn := range optFns {
		if err := optFn(&dag.opts); err != nil {
			return nil, err
		}
	}
	return dag, nil
}

func (dag *DAG) ID() string {
	return dag.id
}

func (dag *DAG) NewTask(taskID string, handler TaskHandler, optFns ...func(opts *TaskOptions) error) (*Task, error) {
	task, err := newTask(dag, taskID, handler, optFns...)
	if err != nil {
		return nil, err
	}
	if err := dag.dependencies.AddVertexByID(taskID, task); err != nil {
		var ide libdag.IDDuplicateError
		if errors.As(err, &ide) {
			return nil, &TaskIDDuplicateError{
				TaskID: taskID,
			}
		}
		return nil, UnknownError{err: err}
	}
	return task, err
}

func (dag *DAG) NewLogger(ctx *DAGRunContext) (*log.Logger, error) {
	if dag.opts.newLoggerFunc == nil {
		return log.Default(), nil
	}
	return dag.opts.newLoggerFunc(ctx)
}

func (dag *DAG) NumOfTasksInSingleInvoke() int {
	if dag.opts.numOfTasksInSingleInvoke <= 0 {
		return 1
	}
	return dag.opts.numOfTasksInSingleInvoke
}

const defaultCircuitBreaker = 10000

func (dag *DAG) CircuitBreaker() int {
	if dag.opts.circuitBreaker <= 0 {
		return defaultCircuitBreaker
	}
	return dag.opts.circuitBreaker
}

func (dag *DAG) AddDependency(ancestor *Task, descendant *Task) error {
	if err := dag.dependencies.AddEdge(ancestor.ID(), descendant.ID()); err != nil {
		var ede libdag.EdgeDuplicateError
		if errors.As(err, &ede) {
			return &TaskDependencyDuplicateError{
				Ancestor:   ancestor,
				Descendant: descendant,
			}
		}
		var sdee libdag.SrcDstEqualError
		if errors.As(err, &sdee) {
			return &AncestorDescendantSameError{
				Ancestor:   ancestor,
				Descendant: descendant,
			}
		}
		var ele libdag.EdgeLoopError
		if errors.As(err, &ele) {
			return &CycleDetectedInDAGError{
				Start: ancestor,
				End:   descendant,
			}
		}
		return UnknownError{err: err}
	}
	return nil
}

func (dag *DAG) GetTask(taskID string) (*Task, bool) {
	v, err := dag.dependencies.GetVertex(taskID)
	if err != nil {
		return nil, false
	}
	return v.(*Task), true
}

func (dag *DAG) GetAllTasks() []*Task {
	vs := dag.dependencies.GetVertices()
	return convertTasks(vs)
}

func (dag *DAG) GetStartTasks() []*Task {
	return convertTasks(dag.dependencies.GetRoots())
}

func (dag *DAG) GetDownstreamTasks(taskID string) []*Task {
	vs, err := dag.dependencies.GetChildren(taskID)
	if err != nil {
		return []*Task{}
	}
	return convertTasks(vs)
}

func (dag *DAG) GetUpstreamTasks(taskID string) []*Task {
	vs, err := dag.dependencies.GetParents(taskID)
	if err != nil {
		return []*Task{}
	}
	return convertTasks(vs)
}

func (dag *DAG) GetAncestorTasks(taskID string) []*Task {
	ancestorTaskIDs, err := dag.dependencies.GetOrderedAncestors(taskID)
	if err != nil {
		return []*Task{}
	}
	return lo.FilterMap(ancestorTaskIDs, func(ancestorTaskID string, _ int) (*Task, bool) {
		return dag.GetTask(ancestorTaskID)
	})
}

func (dag *DAG) GetDescendantTasks(taskID string) []*Task {
	descendantTaskIDs, err := dag.dependencies.GetOrderedDescendants(taskID)
	if err != nil {
		return []*Task{}
	}
	return lo.FilterMap(descendantTaskIDs, func(descendantTaskID string, _ int) (*Task, bool) {
		return dag.GetTask(descendantTaskID)
	})
}

func convertTasks(vs map[string]interface{}) []*Task {
	tasks := lo.Map(lo.Values(vs), func(v interface{}, _ int) *Task {
		return v.(*Task)
	})
	sort.SliceStable(tasks, func(i, j int) bool {
		return tasks[i].id < tasks[j].id
	})
	return tasks
}

func (dag *DAG) GetExecutableTasks(finishedTaskIDs []string) []*Task {
	tasks := dag.GetAllTasks()
	return lo.Filter(tasks, func(task *Task, _ int) bool {
		return !lo.Contains(finishedTaskIDs, task.ID()) && dag.IsExecutableTask(task.ID(), finishedTaskIDs)
	})
}

func (dag *DAG) IsExecutableTask(taskID string, finishedTaskIDs []string) bool {
	upstreamTasks := dag.GetUpstreamTasks(taskID)
	nonFinishedUpstreamTasks := lo.Filter(upstreamTasks, func(task *Task, _ int) bool {
		return !lo.Contains(finishedTaskIDs, task.ID())
	})
	return len(nonFinishedUpstreamTasks) == 0
}

func (dag *DAG) WarkAllDependencies(fn func(ancestor *Task, descendant *Task) error) error {
	tasks := dag.GetAllTasks()
	for _, ancestor := range tasks {
		for _, descendant := range tasks {
			if descendant.ID() == ancestor.ID() {
				continue
			}
			if edge, err := dag.dependencies.IsEdge(ancestor.ID(), descendant.ID()); err != nil {
				return err
			} else if edge {
				if err := fn(ancestor, descendant); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (dag *DAG) Execute(ctx context.Context, dagRunCtx *DAGRunContext) (*DAGRunContext, error) {
	l, err := dag.NewLogger(dagRunCtx)
	if err != nil {
		return dagRunCtx, err
	}
	if dagRunCtx.LambdaCallCount == 0 {
		l.Printf("[info] start new DAG: DAGRunId %s", dagRunCtx.DAGRunID)
	}
	dagRunCtx.LambdaCallCount++
	if dagRunCtx.LambdaCallCount >= dag.CircuitBreaker() {
		l.Printf("[info] DAG run CircuitBreak: DAGRunId %s    Lambda call Count %d", dagRunCtx.DAGRunID, dagRunCtx.LambdaCallCount)
		dagRunCtx.Continue = false
		return dagRunCtx, messages.InvokeResponse_Error{
			Message: fmt.Sprintf("CircuitBreak: lambda call count over %d", dag.CircuitBreaker()),
			Type:    "LambDAG.CircuitBreak",
		}
	}
	finishedTasks := lo.Keys(dagRunCtx.TaskResponses)
	executableTasks := dag.GetExecutableTasks(finishedTasks)
	dagRunCtx.Continue = true
	if len(executableTasks) == 0 {
		now := flextime.Now()
		l.Printf("[info] end DAG: DAGRunId %s    DAG Run duration %s", dagRunCtx.DAGRunID, now.Sub(dagRunCtx.DAGRunStartAt))
		dagRunCtx.Continue = false
		return dagRunCtx, nil
	}
	eg, egCtx := errgroup.WithContext(ctx)
	var mu sync.Mutex
	for i := 0; i < dag.NumOfTasksInSingleInvoke(); i++ {
		if i >= len(executableTasks) {
			break
		}
		task := executableTasks[i]
		eg.Go(func() error {
			taskID := task.ID()
			l.Printf("[info] start task: DAGRunId %s    TaskId %s", dagRunCtx.DAGRunID, taskID)
			resp, err := task.TaskHandler().Invoke(egCtx, &TaskRequest{
				DAGRunID:      dagRunCtx.DAGRunID,
				DAGRunConfig:  dagRunCtx.DAGRunConfig,
				TaskResponses: dagRunCtx.TaskResponses,
			})
			l.Printf("[info] end task: DAGRunId %s    TaskId %s  Success %v", dagRunCtx.DAGRunID, taskID, err == nil)
			if err != nil {
				var tre *TaskRetryableError
				if errors.As(err, &tre) {
					return messages.InvokeResponse_Error{
						Message: tre.Error(),
						Type:    "LambDAG.Retryable",
					}
				}
				return err
			}

			respRawMessage, err := json.Marshal(resp)
			if err != nil {
				return messages.InvokeResponse_Error{
					Message: err.Error(),
					Type:    "LambDAG.ResponseInvalid",
				}
			}
			mu.Lock()
			defer mu.Unlock()
			dagRunCtx.TaskResponses[taskID] = respRawMessage
			finishedTasks = append(finishedTasks, taskID)
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return dagRunCtx, err
	}
	executableTasks = dag.GetExecutableTasks(finishedTasks)
	if len(executableTasks) == 0 {
		now := flextime.Now()
		l.Printf("[info] end DAG: DAGRunId %s    DAG Run duration %s", dagRunCtx.DAGRunID, now.Sub(dagRunCtx.DAGRunStartAt))
		dagRunCtx.Continue = false
	}
	return dagRunCtx, nil
}
