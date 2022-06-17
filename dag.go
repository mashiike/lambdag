package lambdag

import (
	"errors"
	"log"
	"sort"

	libdag "github.com/heimdalr/dag"
	"github.com/samber/lo"
)

type DAG struct {
	id           string
	opts         DAGOptions
	dependencies *libdag.DAG
}

type DAGOptions struct {
	newLoggerFunc func(*DAGRunContext) (*log.Logger, error)
}

func WithDAGLogger(fn func(*DAGRunContext) (*log.Logger, error)) func(opts *DAGOptions) error {
	return func(opts *DAGOptions) error {
		opts.newLoggerFunc = fn
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
