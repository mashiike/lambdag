package lambdag

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Songmu/flextime"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/google/uuid"
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
	DAGRunID        string                     `json:"DAGRunId"`
	DAGRunStartAt   time.Time                  `json:"DAGRunStartAt"`
	DAGRunConfig    json.RawMessage            `json:"DAGRunConfig"`
	TaskResponses   map[string]json.RawMessage `json:"TaskResponses,omitempty"`
	LambdaCallCount int                        `json:"LambdaCallCount"`
	Continue        bool                       `json:"Continue"`
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
	return h.dag.Execute(ctx, &dagRunCtx)
}
