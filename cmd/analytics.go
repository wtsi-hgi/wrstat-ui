/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
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
	"github.com/wtsi-hgi/wrstat-ui/analytics"
)

var serverHost string

var analyticsCmd = &cobra.Command{
	Use:   "analytics",
	Short: "Start analytics server",
	Long: `Start analytics server to view recorded frontend analytics.

Given the path to an sqlite database produced by the server subcommands
--spyware flag will launch a webserver that can be used to explore the captured
data.

The server will, by default, start on port 8080, which can be changed by using
the --bind/-b flag.
`,
	Run: func(_ *cobra.Command, args []string) {
		if len(args) != 1 {
			die("sqlite database location required")
		}

		if err := analytics.StartServer(serverBind, args[0], serverHost); err != nil {
			die("%s", err)
		}
	},
}

func init() {
	analyticsCmd.Flags().StringVarP(&serverBind, "bind", "b", ":8080",
		"address to bind to, eg host:port")
	analyticsCmd.Flags().StringVarP(&serverHost, "host", "H", "http://localhost",
		"address prefix to event URL; should point to active wrstat-ui server")

	RootCmd.AddCommand(analyticsCmd)
}
