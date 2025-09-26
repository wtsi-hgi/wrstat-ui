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

var (
	ErrAmbiguous = errors.New("ambiguous path match combination")
)

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

// StateMachine is a collection of states that can be used to match a
// summary.FileInfo to a particular grouping.
type StateMachine[T any] []charState[T]

// GetGroup matches the given summary.FileInfo to a group, or nil if no group is
// matched.
func (s StateMachine[T]) GetGroup(info *summary.FileInfo) *T {
	return s[s.getState(s.getPathState(info.Path), info.Name)].Group
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

func (s *StateMachine[T]) build(groups []PathGroup[T], state uint32) error {
	ct, err := s.buildCharTable(groups, state)
	if err != nil {
		return err
	}

	return s.buildChildren(ct, state)
}

func (s StateMachine[T]) buildCharTable(groups []PathGroup[T], state uint32) (ct [256][]PathGroup[T], err error) {
	ended := false

	for _, group := range groups {
		if len(group.Path) == 0 { //nolint:nestif
			if ended {
				return ct, ErrAmbiguous
			}

			ended = true
			s[state].Group = group.Group
		} else {
			b := group.shiftPath()
			ct[b] = append(ct[b], group)
		}
	}

	return ct, nil
}

func (s *StateMachine[T]) buildChildren(ct [256][]PathGroup[T], state uint32) error {
	for c, lines := range ct {
		if len(lines) == 0 {
			continue
		}

		nextState := uint32(len(*s)) //nolint:gosec

		(*s)[state].chars[c] = nextState
		*s = append(*s, charState[T]{Group: (*s)[state].Group})

		if err := s.build(lines, nextState); err != nil {
			return err
		}
	}

	return nil
}

func (s *StateMachine[T]) mergeWildcards(state, wildcard uint32) {
	st := &(*s)[state]
	wc := st.chars['*']
	hasWildcard := wc != 0

	if hasWildcard {
		s.mergeWildcards(wc, 0)
		s.merge(wc, wildcard, map[uint32]struct{}{})

		wildcard = wc
	}

	if st.Group == nil {
		st.Group = (*s)[wildcard].Group
	}

	for c, child := range (*s)[state].chars {
		if child == 0 {
			st.chars[c] = wildcard
		} else if c != '*' {
			s.mergeWildcards(child, wildcard)
		}
	}

	if hasWildcard {
		s.fillState(wildcard, wildcard, (*s)[wildcard].Group, map[uint32]struct{}{})
	}
}

func (s *StateMachine[T]) merge(state, oldState uint32, done map[uint32]struct{}) {
	done[state] = struct{}{}
	sc := &(*s)[state]
	nsc := &(*s)[oldState]

	if sc.Group == nil {
		sc.Group = nsc.Group
	}

	for c, child := range sc.chars {
		if child == 0 {
			sc.chars[c] = s.clone(nsc.chars[c], map[uint32]uint32{})
		} else if _, ok := done[child]; !ok {
			s.merge(child, nsc.chars[c], done)
		}
	}
}

func (s *StateMachine[T]) clone(state uint32, done map[uint32]uint32) uint32 {
	if state == 0 {
		return 0
	}

	if d, ok := done[state]; ok {
		return d
	}

	nextState := uint32(len(*s))
	*s = append(*s, charState[T]{Group: (*s)[state].Group})
	done[state] = nextState

	for c, child := range (*s)[state].chars {
		(*s)[nextState].chars[c] = s.clone(child, done)
	}

	return nextState
}

func (s StateMachine[T]) fillState(state, loopState uint32, group *T, done map[uint32]struct{}) {
	done[state] = struct{}{}
	sc := &s[state]
	ls := &s[loopState]

	if sc.Group == nil {
		sc.Group = group
	}

	for c, child := range sc.chars {
		if child == 0 {
			if ls.chars[c] != 0 {
				sc.chars[c] = ls.chars[c]
			} else {
				sc.chars[c] = loopState
			}
		} else if _, ok := done[child]; !ok {
			s.fillState(child, loopState, group, done)
		}
	}
}

// NewStateMachine compiles a StateMachine from the given slice of PathGroups.
//
// Once compiled, the returned StateMachine can be used to match arbitrary paths
// and to get the matching Group data.
//
// Paths can contain wildcards (*) that will match zero or more arbitrary
// characters.
func NewStatemachine[T any](lines []PathGroup[T]) (StateMachine[T], error) {
	states := make(StateMachine[T], 2, 1024) //nolint:mnd

	if err := states.build(lines, 1); err != nil {
		return nil, err
	}

	states.mergeWildcards(1, 0)

	return states, nil
}
