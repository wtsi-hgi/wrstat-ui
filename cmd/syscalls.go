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

package cmd

import (
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/syscalls"
)

const defaultSyscallReloadtime = 10

var syscallLogReloadTime uint

// summariseCmd represents the stat command.
var syscallsCmd = &cobra.Command{
	Use:   "syscalls",
	Short: "Start a server to analyse wrstat syscall logs",
	Long: `Start a server to analyse wrstat syscall logs.

Starting the server brings up a web interface to view statistical information on
the syscalls called during 'wrstat multi' runs. Provide one or more
'wrstat multi -f' output directories to be scanned for logs.

The bind flag (default: ":8080") determines the host and port that the server
runs on.

The reload flag (default: 10) determines the delay in minutes between checking
for new logs in the given directories. A value of zero disables the repeated
checks.
`,
	Run: func(_ *cobra.Command, args []string) {
		if err := syscalls.StartServer(serverBind, syscallLogReloadTime, args...); err != nil {
			die("%s", err)
		}
	},
}

func init() {
	syscallsCmd.Flags().StringVarP(&serverBind, "bind", "b", ":8080",
		"address to bind to, eg host:port")
	syscallsCmd.Flags().UintVarP(&syscallLogReloadTime, "reload", "r", defaultSyscallReloadtime,
		"duration in minutes before checking for new syscall logs to load")

	RootCmd.AddCommand(syscallsCmd)
}
