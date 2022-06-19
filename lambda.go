package lambdag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Songmu/flextime"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambda/messages"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"
)

type Hoge lambda.Handler

type LambdaHandler struct {
	dag *DAG
}

func NewLambdaHandler(dag *DAG) lambda.Handler {
	handler := &LambdaHandler{
		dag: dag,
	}
	return lambda.NewHandler(handler.Invoke)
}

type DAGRunContext struct {
	context.Context `json:"-"`
	DAGRunID        string                     `json:"DAGRunId"`
	DAGRunStartAt   time.Time                  `json:"DAGRunStartAt"`
	DAGRunConfig    json.RawMessage            `json:"DAGRunConfig"`
	TaskResponses   map[string]json.RawMessage `json:"TaskResponses,omitempty"`
	LambdaCallCount int                        `json:"LambdaCallCount"`
	Continue        bool                       `json:"Continue"`
}

const defaultCircuitBreaker = 10000

func (h *LambdaHandler) Invoke(ctx context.Context, payload json.RawMessage) (interface{}, error) {
	var dagRunCtx DAGRunContext
	var startNewDAG bool
	if err := json.Unmarshal(payload, &dagRunCtx); err != nil || dagRunCtx.DAGRunID == "" {
		dagRunCtx.DAGRunConfig = payload
		dagRunCtx.TaskResponses = make(map[string]json.RawMessage)
		uuidObj, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}
		dagRunCtx.DAGRunID = uuidObj.String()
		dagRunCtx.DAGRunStartAt = flextime.Now()
		startNewDAG = true
	}
	dagRunCtx.Context = ctx
	l, err := h.dag.NewLogger(&dagRunCtx)
	if err != nil {
		return nil, err
	}
	if startNewDAG {
		l.Printf("[info] start new DAG: DAGRunId %s", dagRunCtx.DAGRunID)
	}
	dagRunCtx.LambdaCallCount++
	if dagRunCtx.LambdaCallCount >= defaultCircuitBreaker {
		l.Printf("[info] DAG run CircuitBreak: DAGRunId %s    Lambda call Count %d", dagRunCtx.DAGRunID, dagRunCtx.LambdaCallCount)
		dagRunCtx.Continue = false
		bs, _ := json.Marshal(dagRunCtx)
		return bs, messages.InvokeResponse_Error{
			Message: fmt.Sprintf("CircuitBreak: lambda call count over %d", defaultCircuitBreaker),
			Type:    "LambDAG.CircuitBreak",
		}
	}
	finishedTasks := lo.Keys(dagRunCtx.TaskResponses)
	executableTasks := h.dag.GetExecutableTasks(finishedTasks)
	dagRunCtx.Continue = true
	if len(executableTasks) == 0 {
		now := flextime.Now()
		l.Printf("[info] end DAG: DAGRunId %s    DAG Run duration %s", dagRunCtx.DAGRunID, now.Sub(dagRunCtx.DAGRunStartAt))
		dagRunCtx.Continue = false
		return dagRunCtx, nil
	}
	eg, egCtx := errgroup.WithContext(dagRunCtx)
	var mu sync.Mutex
	for i := 0; i < h.dag.NumOfTasksInSingleInvoke(); i++ {
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
	executableTasks = h.dag.GetExecutableTasks(finishedTasks)
	if len(executableTasks) == 0 {
		now := flextime.Now()
		l.Printf("[info] end DAG: DAGRunId %s    DAG Run duration %s", dagRunCtx.DAGRunID, now.Sub(dagRunCtx.DAGRunStartAt))
		dagRunCtx.Continue = false
	}
	return dagRunCtx, nil
}
