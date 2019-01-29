package cmd

import (
	"github.com/dforsyth/donut/client"
	"github.com/dforsyth/donut/cluster"
	"github.com/spf13/cobra"
)

func DelWorkCmd(cls *cluster.Cluster, cli *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:  "delwork [name]",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			name := args[0]

			if err := cli.DeleteWork(cls, name); err != nil {
				panic(err)
			}
		},
	}
}
