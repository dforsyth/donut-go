package main

import (
	"fmt"
	"os"
	"strings"

	// Wait for etcd client v3.4, there is a module import issue.
	"github.com/coreos/etcd/clientv3" // "go.etcd.io/etcd/clientv3"
	"github.com/dforsyth/donut/client"
	"github.com/dforsyth/donut/cluster"
	"github.com/dforsyth/donut/coordinator"
	"github.com/dforsyth/donut/donutctl/cmd"
)

func main() {
	endpoints, found := os.LookupEnv("ETCD_ENDPOINTS")
	if !found {
		fmt.Println("ETCD_ENDPOINTS not set.")
		os.Exit(1)
	}
	endpointsList := strings.Split(endpoints, ",")

	clusterName, found := os.LookupEnv("DONUT_CLUSTER_NAME")
	if !found {
		fmt.Println("DONUT_CLUSTER_NAME")
		os.Exit(1)
	}

	etcd, err := clientv3.New(clientv3.Config{
		Endpoints: endpointsList,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	kv, err := coordinator.NewEtcdCoordinator(etcd)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	cli := client.New(kv)
	// We need a cluster name, but that's it.
	cls := cluster.New(clusterName, "donutctl", nil)
	cmd.NewCmd(cls, cli).Execute()
}
