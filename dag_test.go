package lambdag_test

import (
	"context"
	"sort"
	"testing"

	"github.com/mashiike/lambdag"
	"github.com/stretchr/testify/require"
)

func TestDAG(t *testing.T) {
	dag, err := lambdag.NewDAG("test")
	require.NoError(t, err)
	task1, err := dag.NewTask(
		"task1",
		lambdag.TaskHandlerFunc(func(ctx context.Context, tr *lambdag.TaskRequest) (*lambdag.TaskResponse, error) {
			return &lambdag.TaskResponse{}, nil
		}),
	)
	require.NoError(t, err)
	task2, err := dag.NewTask(
		"task2",
		lambdag.TaskHandlerFunc(func(ctx context.Context, tr *lambdag.TaskRequest) (*lambdag.TaskResponse, error) {
			return &lambdag.TaskResponse{}, nil
		}),
	)
	require.NoError(t, err)
	task3, err := dag.NewTask(
		"task3",
		lambdag.TaskHandlerFunc(func(ctx context.Context, tr *lambdag.TaskRequest) (*lambdag.TaskResponse, error) {
			return &lambdag.TaskResponse{}, nil
		}),
	)
	require.NoError(t, err)
	task4, err := dag.NewTask(
		"task4",
		lambdag.TaskHandlerFunc(func(ctx context.Context, tr *lambdag.TaskRequest) (*lambdag.TaskResponse, error) {
			return &lambdag.TaskResponse{}, nil
		}),
	)
	require.NoError(t, err)
	task5, err := dag.NewTask(
		"task5",
		lambdag.TaskHandlerFunc(func(ctx context.Context, tr *lambdag.TaskRequest) (*lambdag.TaskResponse, error) {
			return &lambdag.TaskResponse{}, nil
		}),
	)
	require.NoError(t, err)
	// task1 ─> task2 ───┓
	//    │              v
	//    └───────────> task3 ──> task4
	//								^
	//                  task5 ──────┘
	err = task1.SetDownstream(task2)
	require.NoError(t, err)
	err = task2.SetDownstream(task3)
	require.NoError(t, err)
	err = task1.SetDownstream(task3)
	require.NoError(t, err)
	err = task3.SetDownstream(task4)
	require.NoError(t, err)
	err = task5.SetDownstream(task4)
	require.NoError(t, err)
	require.EqualValues(t, []*lambdag.Task{task1, task5}, dag.GetStartTasks(), "StartTasks")
	requireTasksMatch(t, []*lambdag.Task{task1, task2, task3, task4, task5}, dag.GetAllTasks(), "AllTasks")
	require.EqualValues(t, []*lambdag.Task{task2, task3}, dag.GetDownstreamTasks("task1"), "DownstreamTasks")
	require.EqualValues(t, []*lambdag.Task{task1, task2}, dag.GetUpstreamTasks("task3"), "UpstreamTasks")
	requireTasksMatch(t, []*lambdag.Task{task3, task5, task2, task1}, dag.GetAncestorTasks("task4"), "AncestorTasks")
	requireTasksMatch(t, []*lambdag.Task{task3, task4}, dag.GetDescendantTasks("task2"), "DescendantTasks")
	require.EqualValues(t, false, dag.IsExecutableTask("task2", []string{}), "task2 is executable? task1 not finished")
	require.EqualValues(t, true, dag.IsExecutableTask("task2", []string{"task1"}), "task2 is executable? task1 finished")
	require.EqualValues(t, false, dag.IsExecutableTask("task3", []string{"task1"}), "task3 is executable? task2 not finished")
	require.EqualValues(t, true, dag.IsExecutableTask("task3", []string{"task1", "task2"}), "task3 is executable? task2 not finished")
	requireTasksMatch(t, []*lambdag.Task{task1, task5}, dag.GetExecutableTasks([]string{}), "first executable tasks")
	requireTasksMatch(t, []*lambdag.Task{task2, task5}, dag.GetExecutableTasks([]string{"task1"}), "executable if task1 finished")
	requireTasksMatch(t, []*lambdag.Task{task3, task5}, dag.GetExecutableTasks([]string{"task1", "task2"}), "executable if task1, task2 finished")
}

func requireTasksMatch(t *testing.T, listA []*lambdag.Task, listB []*lambdag.Task, msgAndArgs ...interface{}) {
	t.Helper()
	sort.SliceStable(listA, func(i, j int) bool {
		return listA[i].ID() > listA[j].ID()
	})
	sort.SliceStable(listB, func(i, j int) bool {
		return listB[i].ID() > listB[j].ID()
	})
	require.EqualValues(t, listA, listB, msgAndArgs...)
}
