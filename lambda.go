package lambdag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Songmu/flextime"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambda/messages"
	"github.com/google/uuid"
)

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
	DAGRunID        string                     `json:"DAGRunId"`
	DAGRunStartAt   time.Time                  `json:"DAGRunStartAt"`
	DAGRunConfig    json.RawMessage            `json:"DAGRunConfig"`
	TaskResponses   map[string]json.RawMessage `json:"TaskResponses,omitempty"`
	LambdaCallCount int                        `json:"LambdaCallCount"`
	Continue        bool                       `json:"Continue"`
	IsCircuitBreak  bool                       `json:"IsCircuitBreak"`
}

func (h *LambdaHandler) Invoke(ctx context.Context, payload json.RawMessage) (interface{}, error) {
	var dagRunCtx DAGRunContext
	if err := json.Unmarshal(payload, &dagRunCtx); err != nil || dagRunCtx.DAGRunID == "" {
		dagRunCtx.DAGRunConfig = payload
		dagRunCtx.TaskResponses = make(map[string]json.RawMessage)
		uuidObj, err := uuid.NewRandom()
		if err != nil {
			return nil, err
		}
		dagRunCtx.DAGRunID = uuidObj.String()
		dagRunCtx.DAGRunStartAt = flextime.Now()
		dagRunCtx.LambdaCallCount = 0
	}
	updatedDAGRunCtx, err := h.dag.Execute(ctx, &dagRunCtx)
	if err != nil {
		var tre *TaskRetryableError
		if errors.As(err, &tre) {
			if h.dag.NumOfTasksInSingleInvoke() > 1 {
				updatedDAGRunCtx.Continue = true
				return updatedDAGRunCtx, nil
			}
			return nil, messages.InvokeResponse_Error{
				Message: tre.Error(),
				Type:    "LambDAG.Retryable",
			}
		}
		var jme *json.MarshalerError
		if errors.As(err, &jme) {
			return nil, messages.InvokeResponse_Error{
				Message: err.Error(),
				Type:    "LambDAG.ResponseInvalid",
			}
		}
		return nil, err
	}
	if updatedDAGRunCtx.IsCircuitBreak {
		return dagRunCtx, messages.InvokeResponse_Error{
			Message: fmt.Sprintf("CircuitBreak: lambda call count over %d", h.dag.CircuitBreaker()),
			Type:    "LambDAG.CircuitBreak",
		}
	}
	return updatedDAGRunCtx, nil
}
