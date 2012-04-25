package main

import (
	"donut"
	"encoding/json"
	"errors"
	"flag"
	"io/ioutil"
	"launchpad.net/gozk/zookeeper"
	"log"
	"os"
	"path"
)

type Ctl struct {
	cfg    *donut.Config
	action string
	zk     *zookeeper.Conn
}

func parseConfig(cfgPath string) (*donut.Config, error) {
	buf, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}
	cfg := &donut.Config{}
	if err := json.Unmarshal(buf, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Ctl) connectToZK() error {
	zk, zkEv, err := zookeeper.Dial(c.cfg.Servers, c.cfg.Timeout)
	if err != nil {
		return err
	}
	ev := <-zkEv
	if ev.State != zookeeper.STATE_CONNECTED {
		errors.New("Failed to connect to Zookeeper servers: " + c.cfg.Servers)
	}
	c.zk = zk
	return nil
}

func (c *Ctl) addWork(cluster, workId, assign string, data map[string]interface{}) error {
	m := make(map[string]interface{})
	for k, v := range data {
		if k == cluster {
			return errors.New("Found reserved key %s in data map: " + k)
		}
		m[k] = v
	}
	if assign != "" {
		m[cluster] = assign
	}

	return donut.CreateWork(cluster, c.zk, c.cfg, workId, data)
}

func (c *Ctl) removeWork(cluster, workId string) error {
	donut.CompleteWork(cluster, c.zk, c.cfg, workId)
	return nil
}

func main() {
	ctl := &Ctl{
		action: os.Args[1],
	}

	var cfgPath, cluster, workId, assign string
	flag.StringVar(&cfgPath, "config", "", "Path to configuration file (will fall back to ~/donut/<cluster>.cfg)")
	flag.StringVar(&cluster, "cluster", "", "Cluster name (required)")
	flag.StringVar(&workId, "workId", "", "Work ID (required)")
	flag.StringVar(&assign, "assign", "", "Work assignee")
	if cluster == "" {
		log.Println("cluster is a required argument")
		flag.Usage()
		return
	}
	if cluster == "" {
		log.Println("workId is a required argument")
		flag.Usage()
		return
	}
	if cfgPath == "" {
		cfgPath = path.Join("~/donut/", cluster+".cfg")
	}

	var err error
	if ctl.cfg, err = parseConfig(cfgPath); err != nil {
		log.Fatalln(err)
	}
	if err = ctl.connectToZK(); err != nil {
		log.Fatalln(err)
	}

	switch ctl.action {
	case "add":
		var inp []byte
		if inp, err = ioutil.ReadAll(os.Stdin); err != nil {
			log.Fatalln(err)
		}

		data := make(map[string]interface{})
		if err = json.Unmarshal(inp, data); err != nil {
			log.Println(err)
		}

		if err = ctl.addWork(cluster, workId, assign, data); err != nil {
			log.Println(err)
		}
	case "remove":
	default:
		log.Printf("%s is not a valid action")
	}
}

func usage() {
	os.Exit(0)
}
