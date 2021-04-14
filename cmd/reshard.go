package cmd

import (
	"github.com/urfave/cli/v2"
	"heckel.io/elastictl/tools"
)

var cmdReshard = &cli.Command{
	Name:      "reshard",
	Aliases:   []string{"r"},
	Usage:     "Reshard index using different shard/replica counts",
	UsageText: "elastictl reshard INDEX",
	Action:    execReshard,
	Flags: []cli.Flag{
		&cli.StringFlag{Name: "host", Aliases: []string{"H"}, Value: "localhost:9200", DefaultText: "localhost:9200", Usage: "override default host"},
		&cli.StringFlag{Name: "search", Aliases: []string{"q"}, Value: "", Usage: "only dump documents matching the given ES query"},
		&cli.StringFlag{Name: "dir", Aliases: []string{"d"}, DefaultText: "current directory", Usage: "directory used to store exported index file"},
		&cli.BoolFlag{Name: "no-keep", Aliases: []string{"K"}, Usage: "delete index file after successful import"},
		&cli.IntFlag{Name: "workers", Aliases: []string{"w"}, Value: 50, Usage: "number of concurrent workers"},
		&cli.IntFlag{Name: "shards", Aliases: []string{"s"}, Value: -1, DefaultText: "no change", Usage: "override the number of shards on index creation"},
		&cli.IntFlag{Name: "replicas", Aliases: []string{"r"}, Value: -1, DefaultText: "no change", Usage: "override the number of replicas on index creation"},
	},
}

func execReshard(c *cli.Context) error {
	host := c.String("host")
	search := c.String("search")
	dir := c.String("dir")
	keep := !c.Bool("no-keep")
	workers := c.Int("workers")
	shards := c.Int("shards")
	replicas := c.Int("replicas")
	if c.NArg() < 1 {
		return cli.Exit("invalid syntax: index missing", 1)
	}
	index := c.Args().Get(0)
	return tools.Reshard(host, index, dir, keep, search, workers, shards, replicas)
}
