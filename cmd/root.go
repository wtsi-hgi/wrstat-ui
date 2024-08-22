/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

// package cmd is the cobra file that enables subcommands and handles
// command-line args.

package cmd

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

const userGroupPerm = 0770

// appLogger is used for logging events in our commands.
var appLogger = log15.New()

// these variables are accessible by all subcommands.
var (
	deployment string
	sudo       bool
)

const connectTimeout = 10 * time.Second

// RootCmd represents the base command when called without any subcommands.
var RootCmd = &cobra.Command{
	Use:   "wrstat-ui",
	Short: "wrstat-ui supplies a user interface to a WRStat database.",
	Long: `wrstat-ui supplies a user interface to a WRStat database.

The 'where' subcommand can be used to find out where data is on disk.

The 'server' subcommand can be used to start the web server.`,
}

// Execute adds all child commands to the root command and sets flags
// appropriately. This is called by main.main(). It only needs to happen once to
// the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		die(err.Error())
	}
}

func init() {
	// set up logging to stderr
	appLogger.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StderrHandler))

	// global flags
	RootCmd.PersistentFlags().StringVar(&deployment,
		"deployment",
		"production",
		"the deployment your wr manager was started with")

	RootCmd.PersistentFlags().BoolVar(&sudo,
		"sudo",
		false,
		"created jobs will run with sudo")
}

// hideGlobalFlags can be used for sub-commands that don't need deployment and
// sudo options.
func hideGlobalFlags(from *cobra.Command, command *cobra.Command, strings []string) {
	if err := RootCmd.Flags().MarkHidden("deployment"); err != nil {
		die("err: %s", err)
	}

	if err := RootCmd.Flags().MarkHidden("sudo"); err != nil {
		die("err: %s", err)
	}

	from.Parent().HelpFunc()(command, strings)
}

// logToFile logs to the given file.
func logToFile(path string) {
	fh, err := log15.FileHandler(path, log15.LogfmtFormat())
	if err != nil {
		warn("Could not log to file [%s]: %s", path, err)

		return
	}

	appLogger.SetHandler(fh)
}

// setCLIFormat logs plain text log messages to STDERR.
func setCLIFormat() {
	appLogger.SetHandler(log15.StreamHandler(os.Stderr, cliFormat()))
}

// cliFormat returns a log15.Format that only prints the plain log msg.
func cliFormat() log15.Format { //nolint:ireturn
	return log15.FormatFunc(func(r *log15.Record) []byte {
		b := &bytes.Buffer{}
		fmt.Fprintf(b, "%s\n", r.Msg)

		return b.Bytes()
	})
}

// cliPrint outputs the message to STDOUT.
func cliPrint(msg string, a ...interface{}) {
	fmt.Fprintf(os.Stdout, msg, a...)
}

// info is a convenience to log a message at the Info level.
func info(msg string, a ...interface{}) {
	appLogger.Info(fmt.Sprintf(msg, a...))
}

// warn is a convenience to log a message at the Warn level.
func warn(msg string, a ...interface{}) {
	appLogger.Warn(fmt.Sprintf(msg, a...))
}

// die is a convenience to log a message at the Error level and exit non zero.
func die(msg string, a ...interface{}) {
	appLogger.Error(fmt.Sprintf(msg, a...))
	os.Exit(1)
}
