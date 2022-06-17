package lambdag

import "fmt"

type UnknownError struct {
	err error
}

func (err UnknownError) Error() string {
	return fmt.Sprintf("unexpected:%s", err.err.Error())
}

func (err UnknownError) Unwrap() error {
	return err.err
}

type TaskIDDuplicateError struct {
	TaskID string
}

func (err *TaskIDDuplicateError) Error() string {
	return fmt.Sprintf("task id `%s` is already exists", err.TaskID)
}

type TaskDependencyDuplicateError struct {
	Ancestor   *Task
	Descendant *Task
}

func (err *TaskDependencyDuplicateError) Error() string {
	return fmt.Sprintf("dependency from `%s` to `%s` already exists", err.Ancestor.ID(), err.Descendant.ID())
}

type AncestorDescendantSameError struct {
	Ancestor   *Task
	Descendant *Task
}

func (err *AncestorDescendantSameError) Error() string {
	return fmt.Sprintf("ancestor `%s` and descendant `%s` is same", err.Ancestor.ID(), err.Descendant.ID())
}

type CycleDetectedInDAGError struct {
	Start *Task
	End   *Task
}

func (err *CycleDetectedInDAGError) Error() string {
	return fmt.Sprintf("cycle detected in DAG: between `%s` to `%s`", err.Start.ID(), err.End.ID())
}

type TaskRetryableError struct {
	err error
}

func (err TaskRetryableError) Error() string {
	return fmt.Sprintf("task retryable:%s", err.err.Error())
}

func (err TaskRetryableError) Unwrap() error {
	return err.err
}

func WrapTaskRetryable(err error) error {
	return &TaskRetryableError{err: err}
}
