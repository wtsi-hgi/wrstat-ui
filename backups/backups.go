package backups

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unsafe"

	"github.com/wtsi-hgi/wrstat-ui/stats"
	"github.com/wtsi-hgi/wrstat-ui/summary"
	"github.com/wtsi-hgi/wrstat-ui/summary/group"
)

const maxPathLength = 4096
const maxFilenameLength = 256

// non-ascii bytes could become \xXX (4x the length at worst), the two
// speech-marks are +2 and a newline is +1.
const maxQuotedPathLength = (maxPathLength+maxFilenameLength)*4 + 2 + 1

type Backup struct {
	sm group.StateMachine[projectAction]
}

type handler struct {
	root    string
	backups map[*projectData]io.WriteCloser
	summary Summary
	quoted  [maxQuotedPathLength]byte
	tmpPath [maxPathLength + maxFilenameLength]byte
}

func (h *handler) Handle(file *summary.FileInfo, group *projectAction) error {
	if group == nil || file.EntryType != stats.FileType && file.EntryType != stats.SymlinkType {
		return nil
	}

	switch group.action {
	case actionBackup, actionTempBackup:
		if err := h.writeBackupFile(file, group); err != nil {
			return err
		}
	}

	h.summary.addFile(file, group)

	return nil
}

func (h *handler) writeBackupFile(file *summary.FileInfo, group *projectAction) error {
	w, ok := h.backups[group.projectData]
	if !ok {
		f, err := os.Create(filepath.Join(h.root, group.Requestor+"_"+group.Name))
		if err != nil {
			return err
		}

		w = f
		h.backups[group.projectData] = w
	}

	_, err := w.Write(append(
		strconv.AppendQuote(
			h.quoted[:0], unsafe.String(&h.tmpPath[0], len(file.Path.AppendTo(h.tmpPath[:0]))),
		), '\n'))

	return err
}

type Errors []error

func (e Errors) Error() string {
	var sb strings.Builder

	for _, err := range e {
		sb.WriteString(err.Error())
		sb.WriteString("\n")
	}

	return sb.String()
}

func (e Errors) Unwrap() []error {
	return e
}

func (h *handler) Close() error {
	var e Errors

	for _, f := range h.backups {
		if err := f.Close(); err != nil {
			e = append(e, err)
		}
	}

	if len(e) > 0 {
		return e
	}

	return nil
}

type projectData struct {
	Faculty, Name, Requestor string
}

type projectRootData struct {
	*projectData
	Root      string
	isPlanned bool
}

type projectAction struct {
	*projectRootData
	action
}

func New(lines []*ReportLine, warnRoots ...string) (*Backup, error) {
	actions := createActions(lines, warnRoots)

	sm, err := group.NewStatemachine(actions)
	if err != nil {
		return nil, err
	}

	return &Backup{sm: sm}, nil
}

func createActions(lines []*ReportLine, warnRoots []string) []group.PathGroup[projectAction] {
	actions := createProjectActions(lines)

	for _, root := range warnRoots {
		actions = append(actions, group.PathGroup[projectAction]{
			Path: []byte(filepath.Join(root, "*")),
			Group: &projectAction{
				projectRootData: &projectRootData{
					Root: root,
				},
				action: actionWarn,
			},
		})
	}

	return actions
}

func createProjectActions(lines []*ReportLine) []group.PathGroup[projectAction] {
	projects := make(map[string]*projectData)
	projectRoots := make(map[string]*projectRootData)
	actions := make([]group.PathGroup[projectAction], len(lines))

	for n, line := range lines {
		projectRoot := getProjectRoot(line, projectRoots, projects)
		projectRoot.isPlanned = projectRoot.isPlanned || projectRoot.Root == filepath.Dir(string(line.Path))
		actions[n] = group.PathGroup[projectAction]{
			Path: line.Path,
			Group: &projectAction{
				projectRootData: projectRoot,
				action:          line.action,
			},
		}
	}

	return addRootActions(projectRoots, actions)
}

func getProjectRoot(
	line *ReportLine,
	projectRoots map[string]*projectRootData,
	projects map[string]*projectData,
) *projectRootData {
	projectRootKey := line.requestor + "\x00" + line.name + "\x00" + line.faculty + "\x00" + filepath.Clean(line.root)

	projectRoot, ok := projectRoots[projectRootKey]
	if !ok {
		projectKey := line.requestor + "\x00" + line.name + "\x00" + line.faculty

		project, ok := projects[projectKey]
		if !ok {
			project = &projectData{
				Name:      line.name,
				Requestor: line.requestor,
				Faculty:   line.faculty,
			}
			projects[projectKey] = project
		}

		projectRoot = &projectRootData{
			projectData: project,
			Root:        line.root,
		}
		projectRoots[projectRootKey] = projectRoot
	}

	return projectRoot
}

func addRootActions(projectRoots map[string]*projectRootData, actions []group.PathGroup[projectAction]) []group.PathGroup[projectAction] {
	for _, project := range projectRoots {
		if !project.isPlanned {
			actions = append(actions, group.PathGroup[projectAction]{
				Path: []byte(filepath.Join(project.Root, "*")),
				Group: &projectAction{
					projectRootData: project,
					action:          actionWarn,
				},
			})
		}
	}

	return actions
}

func (b *Backup) Process(statsData io.Reader, reportRoot string) error {
	s := summary.NewSummariser(stats.NewStatsParser(statsData))

	h := &handler{
		root:    reportRoot,
		backups: make(map[*projectData]io.WriteCloser),
		summary: make(Summary),
	}

	s.AddGlobalOperation(group.NewGrouper(b.sm, h))

	if err := s.Summarise(); err != nil {
		h.Close()

		return err
	}

	return h.Close()
}
