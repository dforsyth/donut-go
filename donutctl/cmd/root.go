package cmd

import (
	"github.com/dforsyth/donut/client"
	"github.com/dforsyth/donut/cluster"
	"github.com/spf13/cobra"
)

func NewCmd(cls *cluster.Cluster, cli *client.Client) *cobra.Command {
	rootCmd := &cobra.Command{
		Use: "donutctl",
	}

	rootCmd.AddCommand(AddWorkCmd(cls, cli))
	rootCmd.AddCommand(DelWorkCmd(cls, cli))

	return rootCmd
}
