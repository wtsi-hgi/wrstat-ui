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

type pathGroup = group.PathGroup[projectAction]

// Backup represents a BackupPlan ready to process wrstat stats data and produce
// backup sets and summaries.
type Backup struct {
	sm      group.StateMachine[projectAction]
	summary backupSummary
}

type handler struct {
	root    string
	backups map[*projectData]io.WriteCloser
	summary backupSummary
	quoted  [maxQuotedPathLength]byte
	tmpPath [maxPathLength + maxFilenameLength]byte
}

func (h *handler) Handle(file *summary.FileInfo, group *projectAction) error {
	if group == nil || file.EntryType != stats.FileType && file.EntryType != stats.SymlinkType {
		return nil
	}

	switch group.Action { //nolint:exhaustive
	case ActionBackup, ActionTempBackup:
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
			h.quoted[:0], unsafe.String(&h.tmpPath[0], len(append(file.Path.AppendTo(h.tmpPath[:0]), file.Name...))),
		), '\n'))

	return err
}

// Errors is a slice of errors.
type Errors []error

// Error implements the error interface.
func (e Errors) Error() string {
	var sb strings.Builder

	for _, err := range e {
		sb.WriteString(err.Error())
		sb.WriteString("\n")
	}

	return sb.String()
}

// Unwrap returns a slice of errors for the errors.Unwrap function.
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
	Root string
}

type projectAction struct {
	*projectRootData
	Action Action
}

// New takes a parsed backup plan CSV, as produced by the ParseCSV function, and
// optional, additional directories for finding unplanned files outside of
// project roots.
//
// On the Backup object returned the Process method can be called multiple times
// to produce FOFNs (File of Filenames) of files to be backed up, and the
// Summarise method can be called to produce a JSON summary of the files
// processed so far.
func New(lines []*ReportLine, warnRoots ...string) (*Backup, error) {
	actions := createActions(lines, warnRoots)

	sm, err := group.NewStatemachine(actions)
	if err != nil {
		return nil, err
	}

	return &Backup{
		sm:      sm,
		summary: make(backupSummary),
	}, nil
}

func createActions(lines []*ReportLine, warnRoots []string) []pathGroup {
	actions := createProjectActions(lines)

	for _, root := range warnRoots {
		actions = append(actions, pathGroup{
			Path: []byte(filepath.Join(root, "*")),
			Group: &projectAction{
				projectRootData: &projectRootData{
					Root: root,
				},
				Action: ActionWarn,
			},
		})
	}

	return actions
}

func createProjectActions(lines []*ReportLine) []pathGroup {
	projects := make(map[string]*projectData)
	projectRoots := make(map[string]*projectRootData)
	actions := make([]pathGroup, len(lines))

	for n, line := range lines {
		projectRoot := getProjectRoot(line, projectRoots, projects)
		actions[n] = pathGroup{
			Path: line.Path,
			Group: &projectAction{
				projectRootData: projectRoot,
				Action:          line.Action,
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
	projectRootKey := line.Requestor + "\x00" + line.Name + "\x00" + line.Faculty + "\x00" + filepath.Clean(line.Root)

	projectRoot, ok := projectRoots[projectRootKey]
	if !ok {
		projectKey := line.Requestor + "\x00" + line.Name + "\x00" + line.Faculty

		project, ok := projects[projectKey]
		if !ok {
			project = &projectData{
				Name:      line.Name,
				Requestor: line.Requestor,
				Faculty:   line.Faculty,
			}
			projects[projectKey] = project
		}

		projectRoot = &projectRootData{
			projectData: project,
			Root:        line.Root,
		}
		projectRoots[projectRootKey] = projectRoot
	}

	return projectRoot
}

func addRootActions(projectRoots map[string]*projectRootData, actions []pathGroup) []pathGroup {
	done := make(map[string]struct{})

	for _, action := range actions {
		done[string(action.Path)] = struct{}{}
	}

	for _, project := range projectRoots {
		root := filepath.Join(project.Root, "*")

		if _, ok := done[root]; !ok {
			actions = append(actions, pathGroup{
				Path: []byte(root),
				Group: &projectAction{
					projectRootData: project,
					Action:          ActionWarn,
				},
			})
			done[root] = struct{}{}
		}
	}

	return actions
}

// Process summarises the given stats data, produces FOFNs (File of Filenames)
// of files to be backed up and adds to the summary data which can be written
// with the Summarise method.
//
// Backup FOFNs are written to the reportRoot directory with a file for each
// requestor+project pair with the filename being `{requestor}_${projectname}`.
func (b *Backup) Process(statsData io.Reader, reportRoot string) error {
	s := summary.NewSummariser(stats.NewStatsParser(statsData))

	h := &handler{
		root:    reportRoot,
		backups: make(map[*projectData]io.WriteCloser),
		summary: b.summary,
	}

	s.AddGlobalOperation(group.NewGrouper(b.sm, h))

	if err := s.Summarise(); err != nil {
		h.Close()

		return err
	}

	return h.Close()
}

// Summarise writes JSON to the given writer with a row for each
// root+user+action combination. Each rows with contain the following fields:
//
//	Faculty   // Absent in warn root entries
//	Name      // Absent in warn root entries
//	Requestor // Absent in warn root entries
//	Root
//	Action
//	UserID
//	Base
//	Size
//	Count
//	OldestMTime
//	NewestMTime
func (b *Backup) Summarise(w io.Writer) error {
	_, err := b.summary.WriteTo(w)

	return err
}
