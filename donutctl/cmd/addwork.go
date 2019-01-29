package cmd

import (
	"fmt"

	"github.com/dforsyth/donut/client"
	"github.com/dforsyth/donut/cluster"
	"github.com/spf13/cobra"
)

func AddWorkCmd(cls *cluster.Cluster, cli *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:  "addwork [name] [value]",
		Args: cobra.MinimumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			name := args[0]
			value := args[1]

			workKey, err := cli.CreateWork(cls, name, value)
			if err != nil {
				panic(err)
			}
			fmt.Println(workKey)
		},
	}
}
