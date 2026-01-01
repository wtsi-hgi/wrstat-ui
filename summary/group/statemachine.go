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

import (
	"errors"
	"unsafe"

	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var ErrAmbiguous = errors.New("ambiguous path match combination")

type charState[T any] struct {
	chars [256]uint32
	Group *T
}

// PathGroup is a generic type that contains a path, to be matched, and the
// resultant group data to be returned. The path can contain wildcard (*) chars,
// which will match against zero of more characters.
type PathGroup[T any] struct {
	Path  []byte
	Group *T
}

func (p *PathGroup[T]) shiftPath() byte {
	if len(p.Path) == 0 {
		return 0
	}

	b := p.Path[0]
	p.Path = p.Path[1:]

	return b
}

// NewStateMachine compiles a StateMachine from the given slice of PathGroups.
//
// Once compiled, the returned StateMachine can be used to match arbitrary paths
// and to get the matching Group data.
//
// Paths can contain wildcards (*) that will match zero or more arbitrary
// characters.
func NewStatemachine[T any](lines []PathGroup[T]) (StateMachine[T], error) {
	b := &builder[T]{sm: make(StateMachine[T], 2, 1024)} //nolint:mnd

	b.queueBuild(1, 0, lines, nil)

	if err := b.buildQueue(); err != nil {
		return nil, err
	}

	return b.sm, nil
}

// State represents a partial match that can be continued or completed.
type State[T any] struct {
	s     StateMachine[T]
	state uint32
}

// GetState continues matching with the given match bytes, returning a new
// State.
func (s State[T]) GetState(match []byte) State[T] {
	return State[T]{
		s:     s.s,
		state: s.s.getState(s.state, match),
	}
}

// GetStateString acts like GetState, but takes a String.
func (s State[T]) GetStateString(match string) State[T] {
	return s.GetState(unsafe.Slice(unsafe.StringData(match), len(match)))
}

// GetGroup returns the group at the current state.
func (s State[T]) GetGroup() *T {
	return s.s[s.state].Group
}

// IsUnmatched returns true if we're in state 0, which will always return a nil
// group.
func (s State[T]) IsUnmatched() bool {
	return s.state == 0
}

// StateMachine is a collection of states that can be used to match a
// summary.FileInfo to a particular grouping.
type StateMachine[T any] []charState[T]

// GetGroup matches the given summary.FileInfo to a group, or nil if no group is
// matched.
func (s StateMachine[T]) GetGroup(info *summary.FileInfo) *T {
	return s[s.getState(s.getPathState(info.Path), info.Name)].Group
}

// GetState create a intermediary state given the initial match bytes.
func (s StateMachine[T]) GetState(match []byte) State[T] {
	return State[T]{
		s:     s,
		state: s.getState(1, match),
	}
}

// GetStateString acts like GetState, but takes a String.
func (s StateMachine[T]) GetStateString(match string) State[T] {
	return s.GetState(unsafe.Slice(unsafe.StringData(match), len(match)))
}

func (s StateMachine[T]) getPathState(path *summary.DirectoryPath) uint32 {
	if path == nil {
		return 1
	}

	return s.getState(s.getPathState(path.Parent), unsafe.Slice(unsafe.StringData(path.Name), len(path.Name)))
}

func (s StateMachine[T]) getState(state uint32, path []byte) uint32 {
	for _, c := range path {
		state = s[state].chars[c]
	}

	return state
}

type stateLines[T any] struct {
	state, wc      uint32
	lines, wcLines []PathGroup[T]
}

type builder[T any] struct {
	sm    StateMachine[T]
	queue []stateLines[T]
}

func (b *builder[T]) build(state, wildcard uint32, groups, wildcardGroups []PathGroup[T]) error {
	ct, err := b.buildCharTable(groups, state)
	if err != nil {
		return err
	}

	if ct['*'] != nil {
		var err error

		if wildcardGroups, err = b.buildWildcard(ct['*'], state, wildcardGroups); err != nil {
			return err
		}

		wildcard = b.sm[state].chars['*']
	}

	if b.sm[state].Group == nil {
		b.sm[state].Group = b.sm[wildcard].Group
	}

	b.buildChildren(ct, state, wildcard, wildcardGroups)

	return nil
}

func (b *builder[T]) buildCharTable(groups []PathGroup[T], state uint32) (ct [256][]PathGroup[T], err error) {
	ended := false

	for _, group := range groups {
		if group.Group == nil {
			continue
		}

		if len(group.Path) == 0 { //nolint:nestif
			if ended {
				return ct, ErrAmbiguous
			}

			ended = true
			b.sm[state].Group = group.Group
		} else {
			b := group.shiftPath()
			ct[b] = append(ct[b], group)
		}
	}

	return ct, nil
}

func (b *builder[T]) buildChildren(
	ct [256][]PathGroup[T], state, wildcard uint32, wildcardGroups []PathGroup[T],
) {
	for c, lines := range ct {
		if c == '*' {
			b.sm[state].chars[c] = wildcard

			continue
		}

		wc := b.sm[wildcard].chars[c]

		if len(lines) == 0 {
			b.sm[state].chars[c] = wc

			continue
		}

		nextState := b.newState()
		b.sm[state].chars[c] = nextState

		b.queueBuild(nextState, wc, lines, wildcardGroups)
	}
}

func (b *builder[T]) queueBuild(state, wildcard uint32, lines, wcLines []PathGroup[T]) {
	b.queue = append(b.queue, stateLines[T]{
		state:   state,
		wc:      wildcard,
		lines:   lines,
		wcLines: wcLines,
	})
}

func (b *builder[T]) newState() uint32 {
	nextState := uint32(len(b.sm)) //nolint:gosec
	b.sm = append(b.sm, charState[T]{})

	return nextState
}

func (b *builder[T]) buildWildcard(
	groups []PathGroup[T], state uint32, wildcardGroups []PathGroup[T],
) ([]PathGroup[T], error) {
	nextState := b.newState()
	b.sm[state].chars['*'] = nextState

	if err := b.buildWildcardSM(nextState, groups); err != nil {
		return nil, err
	}

	if len(wildcardGroups) == 0 {
		return groups, nil
	}

	curr := len(groups)

	if groups = b.filterWildcardGroups(groups, nextState, wildcardGroups); curr == len(groups) {
		return groups, nil
	}

	b.sm = b.sm[:nextState+1]

	b.loopState(nextState)

	if err := b.buildWildcardSM(nextState, groups); err != nil {
		return nil, err
	}

	return groups, nil
}

func (b *builder[T]) buildWildcardSM(nextState uint32, groups []PathGroup[T]) error {
	c := &builder[T]{sm: b.sm}

	b.loopState(nextState)
	c.queueBuild(nextState, nextState, groups, groups)

	if err := c.buildQueue(); err != nil {
		return err
	}

	b.sm = c.sm

	return nil
}

func (b *builder[T]) loopState(state uint32) {
	chars := &b.sm[state].chars

	for c := range chars {
		chars[c] = state
	}
}

func (b *builder[T]) filterWildcardGroups(
	groups []PathGroup[T], state uint32, wildcardGroups []PathGroup[T],
) []PathGroup[T] {
	for _, group := range wildcardGroups {
		if b.sm[b.sm.getState(state, group.Path)].Group == nil {
			groups = append(groups, group)
		}
	}

	return groups
}

func (b *builder[T]) buildQueue() error {
	for len(b.queue) > 0 {
		item := b.queue[0]

		if err := b.build(item.state, item.wc, item.lines, item.wcLines); err != nil {
			return err
		}

		b.queue = b.queue[1:]
	}

	return nil
}
