/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package group

import "github.com/wtsi-hgi/wrstat-ui/summary"

// Handler is used by NewGrouper to pass each entry of the Summarised stats files
// combined with the discovered group.
type Handler[T any] interface {
	Handle(file *summary.FileInfo, group *T) error
}

// NewGrouper provides a summary.Operation that matches each entry passed from
// the summary.Summariser to a group as defined in the given StateMachine, as
// created with NewStatemachine().
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
