package backups

import (
	"errors"
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

func (s StateMachine) GetLine(path []byte) *Line {
	state := uint32(1)

	for _, c := range path {
		state = s[state].chars[c]
	}

	return s[state].Line
}

type lineBytes struct {
	line      *Line
	directory []byte
}

func (s *StateMachine) build(lines []*Line, state, loopState uint32) error {
	ct, wild, err := s.buildCharTable(lines, state)
	if err != nil {
		return err
	}

	if err := s.buildChildren(ct, state, loopState); err != nil {
		return err
	}

	return s.buildWildcards(wild, state, loopState)
}

func (s StateMachine) buildCharTable(lines []*Line, state uint32) (ct [256][]*Line, wild []lineBytes, err error) {
	ended := false

	for _, line := range lines {
		if len(line.Path) == 0 {
			if ended {
				return ct, nil, ErrAmbiguous
			}

			ended = true
			s[state].Line = line

		} else {

			b := line.shiftPath()

			if b == '*' {
				wild = append(wild, lineBytes{line, line.Path})

				if len(line.Path) > 0 {
					b = line.shiftPath()
				} else if !ended {
					s[state].Line = line
				}
			}

			if b != '*' {
				ct[b] = append(ct[b], line)
			}
		}
	}

	return ct, wild, nil
}

func (s *StateMachine) buildChildren(ct [256][]*Line, state, loopState uint32) error {
	for c, lines := range ct {
		if len(lines) == 0 {
			continue
		}

		nextState := uint32(len(*s))
		(*s)[state].chars[c] = nextState
		*s = append(*s, NewState(loopState, (*s)[state].Line))

		if err := s.build(lines, nextState, loopState); err != nil {
			return err
		}
	}

	return nil
}

func (s *StateMachine) buildWildcards(wild []lineBytes, state, loopState uint32) error {
	if len(wild) == 0 {
		return nil
	}

	newLoopState := uint32(len(*s))
	*s = append(*s, NewState(newLoopState, (*s)[state].Line))

	wildLines := make([]*Line, len(wild))

	for n, w := range wild {
		w.line.Path = w.directory
		wildLines[n] = w.line
	}

	if err := s.build(wildLines, newLoopState, newLoopState); err != nil {
		return err
	}

	s.fillState(state, loopState, newLoopState, (*s)[newLoopState].Line, make(map[uint32]struct{}))

	return nil
}

func (s StateMachine) fillState(state, oldLoopState, newLoopState uint32, line *Line, done map[uint32]struct{}) {
	done[state] = struct{}{}
	sc := &s[state]
	chars := &sc.chars

	if sc.Line == nil {
		sc.Line = line
	}

	for n, cs := range chars {
		if cs == 0 || cs == oldLoopState {
			chars[n] = newLoopState
		} else if _, ok := done[cs]; !ok {
			s.fillState(cs, oldLoopState, newLoopState, line, done)
		}
	}
}

func NewStatemachine(lines []*Line) (StateMachine, error) {
	states := make(StateMachine, 2, 1024)

	if err := states.build(lines, 1, 0); err != nil {
		return nil, err
	}

	return states, nil
}
