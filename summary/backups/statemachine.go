package backups

import (
	"errors"
	"unsafe"
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
	*line
}

func NewState(state uint32, line *line) State {
	s := State{line: line}

	if state != 0 {
		for n := range s.chars {
			s.chars[n] = state
		}
	}

	return s
}

type line struct {
	Path []byte
	action
	reporter string
	name     string
	root     string
}

func (l *line) Action() action {
	if l == nil {
		return actionWarn
	}

	return l.action
}

func (l *line) shiftPath() byte {
	if len(l.Path) == 0 {
		return 0
	}

	b := l.Path[0]
	l.Path = l.Path[1:]

	return b
}

type StateMachine []State

func (s StateMachine) GetLine(path string) *line {
	state := uint32(1)

	for _, c := range unsafe.Slice(unsafe.StringData(path), len(path)) {
		state = s[state].chars[c]
	}

	return s[state].line
}

type lineBytes struct {
	line      *line
	directory []byte
}

func (s *StateMachine) build(lines []*line, state, loopState uint32) error {
	ct, wild, err := s.buildCharTable(lines, state)
	if err != nil {
		return err
	}

	if err := s.buildChildren(ct, state, loopState); err != nil {
		return err
	}

	return s.buildWildcards(wild, state, loopState)
}

func (s StateMachine) buildCharTable(lines []*line, state uint32) (ct [256][]*line, wild []lineBytes, err error) {
	ended := false

	for _, line := range lines {
		if len(line.Path) == 0 {
			if ended {
				return ct, nil, ErrAmbiguous
			}

			ended = true
			s[state].line = line

		} else {

			b := line.shiftPath()

			if b == '*' {
				wild = append(wild, lineBytes{line, line.Path})

				if len(line.Path) > 0 {
					b = line.shiftPath()
				} else if !ended {
					s[state].line = line
				}
			}

			if b != '*' {
				ct[b] = append(ct[b], line)
			}
		}
	}

	return ct, wild, nil
}

func (s *StateMachine) buildChildren(ct [256][]*line, state, loopState uint32) error {
	for c, lines := range ct {
		if len(lines) == 0 {
			continue
		}

		nextState := uint32(len(*s))
		(*s)[state].chars[c] = nextState
		*s = append(*s, NewState(loopState, (*s)[state].line))

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
	*s = append(*s, NewState(newLoopState, (*s)[state].line))

	wildLines := make([]*line, len(wild))

	for n, w := range wild {
		w.line.Path = w.directory
		wildLines[n] = w.line
	}

	if err := s.build(wildLines, newLoopState, newLoopState); err != nil {
		return err
	}

	s.fillState(state, loopState, newLoopState, (*s)[newLoopState].line, make(map[uint32]struct{}))

	return nil
}

func (s StateMachine) fillState(state, oldLoopState, newLoopState uint32, line *line, done map[uint32]struct{}) {
	done[state] = struct{}{}
	sc := &s[state]
	chars := &sc.chars

	if sc.line == nil {
		sc.line = line
	}

	for n, cs := range chars {
		if cs == 0 || cs == oldLoopState {
			chars[n] = newLoopState
		} else if _, ok := done[cs]; !ok {
			s.fillState(cs, oldLoopState, newLoopState, line, done)
		}
	}
}

func NewStatemachine(lines []*line) (StateMachine, error) {
	states := make(StateMachine, 2, 1024)

	if err := states.build(lines, 1, 0); err != nil {
		return nil, err
	}

	return states, nil
}
