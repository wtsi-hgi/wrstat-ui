package group

import "github.com/wtsi-hgi/wrstat-ui/summary"

type Handler[T any] interface {
	Handle(file *summary.FileInfo, group *T) error
}

func NewGrouper[T any](sm StateMachine[T], output Handler[T]) summary.OperationGenerator {
	return func() summary.Operation {
		return &grouper[T]{sm: sm, output: output}
	}
}

type grouper[T any] struct {
	sm     StateMachine[T]
	output Handler[T]
}

func (g *grouper[T]) Add(info *summary.FileInfo) error {
	return g.output.Handle(info, g.sm.GetGroup(info))
}

func (*grouper[T]) Output() error {
	return nil
}
