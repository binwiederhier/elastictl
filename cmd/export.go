package cmd

import (
	"github.com/urfave/cli/v2"
	"heckel.io/elastictl/tools"
)

var cmdExport = &cli.Command{
	Name:      "export",
	Aliases:   []string{"e"},
	Usage:     "Export an entire index to STDOUT",
	UsageText: "elastictl export INDEX",
	Action:    execExport,
	Flags: []cli.Flag{
		&cli.StringFlag{Name: "host", Aliases: []string{"H"}, Value: "localhost:9200", DefaultText: "localhost:9200", Usage: "override default host"},
		&cli.StringFlag{Name: "search", Aliases: []string{"q"}, Value: "", Usage: "only dump documents matching the given ES query"},
	},
}

func execExport(c *cli.Context) error {
	host := c.String("host")
	search := c.String("search")
	if c.NArg() < 1 {
		return cli.Exit("invalid syntax: index missing", 1)
	}
	index := c.Args().Get(0)
	return tools.Export(host, index, search, c.App.Writer)
}
