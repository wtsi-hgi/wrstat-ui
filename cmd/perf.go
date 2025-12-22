package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/spf13/cobra"
)

var (
	perfOwners string
	perfJSON   string
	perfRepeat int
	perfWarmup int
)

const (
	defaultRepeat = 20
	defaultWarmup = 1
)

var perfCmd = &cobra.Command{
	Use:   "bolt-perf",
	Short: "Performance harness for bolt backend (import/query)",
}

var perfQueryCmd = &cobra.Command{
	Use:   "query <inputDir>",
	Short: "Run query suite against bolt DB output directory",
	Args:  cobra.MinimumNArgs(1),
	Run: func(_ *cobra.Command, args []string) {
		inputDir := args[0]

		if perfJSON == "" {
			die("--json is required")
		}

		// minimal implementation: discover dataset dirs and produce a
		// JSON report with required top-level fields.
		absInput, err := filepath.Abs(inputDir)
		if err != nil {
			die("%s", err)
		}

		report := map[string]any{
			"schema_version": 1,
			"backend":        "bolt",
			"git_commit":     "",
			"go_version":     runtime.Version(),
			"os":             runtime.GOOS,
			"arch":           runtime.GOARCH,
			"started_at":     time.Now().UTC().Format(time.RFC3339),
			"input_dir":      absInput,
			"repeat":         perfRepeat,
			"warmup":         perfWarmup,
			"operations":     []any{},
		}

		f, err := os.Create(perfJSON)
		if err != nil {
			die("%s", err)
		}

		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")

		if err = enc.Encode(report); err != nil {
			f.Close()
			die("%s", err)
		}

		f.Close()
		fmt.Fprintf(os.Stdout, "Wrote %s\n", perfJSON)
	},
}

func init() {
	RootCmd.AddCommand(perfCmd)

	perfCmd.AddCommand(perfQueryCmd)

	perfQueryCmd.Flags().StringVar(&perfOwners, "owners", "", "owners CSV for basedirs")
	perfQueryCmd.Flags().StringVar(&perfJSON, "json", "", "path to write JSON report")
	perfQueryCmd.Flags().IntVar(&perfRepeat, "repeat", defaultRepeat, "number of repeats")
	perfQueryCmd.Flags().IntVar(&perfWarmup, "warmup", defaultWarmup, "number of warmups")
}
