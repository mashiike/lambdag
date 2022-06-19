package lambdag_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/Songmu/flextime"
	"github.com/mashiike/lambdag"
	"github.com/stretchr/testify/require"
)

func TestLambdaHandlerSimpleDAG(t *testing.T) {

	dag, err := lambdag.NewDAG(
		"SimpleDAG",
		lambdag.WithNumOfTasksInSingleInvoke(2),
	)
	require.NoError(t, err)
	var dagRunID string

	handleTasks := make([]string, 0, 4)
	var mu sync.Mutex

	task1, err := dag.NewTask("task1", lambdag.TaskHandlerFunc(func(ctx context.Context, tr *lambdag.TaskRequest) (interface{}, error) {
		mu.Lock()
		defer mu.Unlock()
		dagRunID = tr.DAGRunID
		handleTasks = append(handleTasks, "task1")
		return "task1 success", nil
	}))
	require.NoError(t, err)

	task2, err := dag.NewTask("task2", lambdag.TaskHandlerFunc(func(ctx context.Context, tr *lambdag.TaskRequest) (interface{}, error) {
		mu.Lock()
		defer mu.Unlock()
		handleTasks = append(handleTasks, "task2")
		return "task2 success", nil
	}))
	require.NoError(t, err)

	task3, err := dag.NewTask("task3", lambdag.TaskHandlerFunc(func(ctx context.Context, tr *lambdag.TaskRequest) (interface{}, error) {
		mu.Lock()
		defer mu.Unlock()
		handleTasks = append(handleTasks, "task3")
		return "task3 success", nil
	}))
	require.NoError(t, err)

	task4, err := dag.NewTask("task4", lambdag.TaskHandlerFunc(func(ctx context.Context, tr *lambdag.TaskRequest) (interface{}, error) {
		mu.Lock()
		defer mu.Unlock()
		handleTasks = append(handleTasks, "task4")
		return "task4 success", nil
	}))
	require.NoError(t, err)

	err = task1.SetDownstream(task2)
	require.NoError(t, err)
	err = task1.SetDownstream(task3)
	require.NoError(t, err)
	err = task4.SetUpstream(task3)
	require.NoError(t, err)
	err = task4.SetUpstream(task2)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fixedTime := time.Date(2022, 06, 19, 9, 00, 00, 0, time.UTC)
	restore := flextime.Set(fixedTime)
	defer restore()

	handler := lambdag.NewLambdaHandler(dag)
	var dagRunCtx lambdag.DAGRunContext
	dagRunConfig := []byte(`{"Comment":"input your DAG run config here"}`)
	payload := dagRunConfig
	for i := 0; i < 3; i++ {
		resp, err := handler.Invoke(ctx, payload)
		require.NoError(t, err)
		t.Logf("invoke[%d] resp: %s", i, string(resp))
		require.NoError(t, json.Unmarshal(resp, &dagRunCtx))
		if !dagRunCtx.Continue {
			break
		}
		payload = resp
	}
	require.ElementsMatch(t, []string{"task1", "task2", "task3", "task4"}, handleTasks)
	require.EqualValues(t, fixedTime.Format(time.RFC3339), dagRunCtx.DAGRunStartAt.Format(time.RFC3339))
	expectedDAGRunCtx := lambdag.DAGRunContext{
		DAGRunID:      dagRunID,
		DAGRunConfig:  dagRunConfig,
		DAGRunStartAt: dagRunCtx.DAGRunStartAt,
		TaskResponses: map[string]json.RawMessage{
			"task1": json.RawMessage(`"task1 success"`),
			"task2": json.RawMessage(`"task2 success"`),
			"task3": json.RawMessage(`"task3 success"`),
			"task4": json.RawMessage(`"task4 success"`),
		},
		LambdaCallCount: 3,
		Continue:        false,
	}
	require.EqualValues(t, expectedDAGRunCtx, dagRunCtx)
}
