package backups

import (
	"errors"
	"unsafe"

	"github.com/wtsi-hgi/wrstat-ui/summary"
)

var (
	ErrAmbiguous = errors.New("ambiguous")
)

type action uint8

const (
	actionWarn action = iota
	actionNoBackup
	actionTempBackup
	actionBackup
)

type State struct {
	chars [256]uint32
	*Line
}

func NewState(state uint32, line *Line) State {
	s := State{Line: line}

	if state != 0 {
		for n := range s.chars {
			s.chars[n] = state
		}
	}

	return s
}

type StateMachine []State

func (s StateMachine) GetLine(info *summary.FileInfo) *Line {
	return s[s.getState(s.getPathState(info.Path), info.Name)].Line
}

func (s StateMachine) getPathState(path *summary.DirectoryPath) uint32 {
	if path == nil {
		return 1
	}

	return s.getState(s.getPathState(path.Parent), unsafe.Slice(unsafe.StringData(path.Name), len(path.Name)))
}

func (s StateMachine) getState(state uint32, path []byte) uint32 {
	for _, c := range path {
		state = s[state].chars[c]
	}

	return state
}

type lineBytes struct {
	line      *Line
	directory []byte
}

func (s *StateMachine) build(lines []*Line, state uint32) error {
	ct, err := s.buildCharTable(lines, state)
	if err != nil {
		return err
	}

	if err := s.buildChildren(ct, state); err != nil {
		return err
	}

	s.buildWildcards(state)

	return nil
}

func (s StateMachine) buildCharTable(lines []*Line, state uint32) (ct [256][]*Line, err error) {
	ended := false

	for _, line := range lines {
		if len(line.Path) == 0 {
			if ended {
				return ct, ErrAmbiguous
			}

			ended = true
			s[state].Line = line
		} else {
			b := line.shiftPath()
			ct[b] = append(ct[b], line)
		}
	}

	return ct, nil
}

func (s *StateMachine) buildChildren(ct [256][]*Line, state uint32) error {
	for c, lines := range ct {
		if len(lines) == 0 {
			continue
		}

		nextState := uint32(len(*s))

		(*s)[state].chars[c] = nextState
		*s = append(*s, State{Line: (*s)[state].Line})

		if err := s.build(lines, nextState); err != nil {
			return err
		}
	}

	return nil
}

func (s *StateMachine) buildWildcards(state uint32) {
	newState := (*s)[state].chars['*']

	if newState == 0 {
		return
	}

	s.merge(state, newState, newState, make(map[uint32]struct{}))
	s.fillState(state, newState, (*s)[state].Line, make(map[uint32]struct{}))
}

func (s *StateMachine) merge(state, oldLoopState, loopState uint32, done map[uint32]struct{}) {
	done[state] = struct{}{}
	sc := &(*s)[state]
	nsc := (*s)[loopState]

	if sc.Line == nil {
		sc.Line = nsc.Line
	}

	for c, child := range sc.chars {
		if child == 0 {
			sc.chars[c] = (*s)[oldLoopState].chars[c]
		} else if _, ok := done[child]; !ok {
			s.merge(child, oldLoopState, nsc.chars[c], done)
		}
	}
}

func (s StateMachine) fillState(state, loopState uint32, line *Line, done map[uint32]struct{}) {
	done[state] = struct{}{}
	sc := &s[state]
	chars := &sc.chars

	if sc.Line == nil {
		sc.Line = line
	}

	for n, child := range chars {
		if child == 0 {
			chars[n] = loopState
		} else if _, ok := done[child]; !ok {
			s.fillState(child, loopState, line, done)
		}
	}
}

func NewStatemachine(lines []*Line) (StateMachine, error) {
	states := make(StateMachine, 2, 1024)

	if err := states.build(lines, 1); err != nil {
		return nil, err
	}

	return states, nil
}
