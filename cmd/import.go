package cmd

import (
	"github.com/urfave/cli/v2"
	"heckel.io/elastictl/tools"
)

var cmdBlast = &cli.Command{
	Name:      "import",
	Aliases:   []string{"i"},
	Usage:     "Write to ES index from STDIN",
	UsageText: "elastictl import INDEX",
	Action:    execImport,
	Flags: []cli.Flag{
		&cli.StringFlag{Name: "host", Aliases: []string{"H"}, Value: "localhost:9200", DefaultText: "localhost:9200", Usage: "override default host"},
		&cli.IntFlag{Name: "workers", Aliases: []string{"w"}, Value: 50, Usage: "number of concurrent workers"},
		&cli.IntFlag{Name: "shards", Aliases: []string{"s"}, Value: -1, DefaultText: "no change", Usage: "override the number of shards on index creation"},
		&cli.IntFlag{Name: "replicas", Aliases: []string{"r"}, Value: -1, DefaultText: "no change", Usage: "override the number of replicas on index creation"},
		&cli.BoolFlag{Name: "no-create", Aliases: []string{"N"}, Value: false, Usage: "do not create index"},
	},
}

func execImport(c *cli.Context) error {
	host := c.String("host")
	workers := c.Int("workers")
	nocreate := c.Bool("no-create")
	shards := c.Int("shards")
	replicas := c.Int("replicas")
	if c.NArg() < 1 {
		return cli.Exit("invalid syntax: index missing", 1)
	}
	index := c.Args().Get(0)
	_, err := tools.Import(host, index, workers, nocreate, shards, replicas, c.App.Reader, -1)
	return err
}
