package cmd

import (
	"github.com/spf13/cobra"
	"github.com/wtsi-hgi/wrstat-ui/analytics"
)

var analyticsCmd = &cobra.Command{
	Use:   "analytics",
	Short: "Start analytics server",
	Long:  `Start analytics server.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("sqlite database location required")
		}

		if err := analytics.StartServer(serverBind, args[0]); err != nil {
			die("%s", err)
		}
	},
}

func init() {
	analyticsCmd.Flags().StringVarP(&serverBind, "bind", "b", ":8080",
		"address to bind to, eg host:port")

	RootCmd.AddCommand(analyticsCmd)
}
