// Package cmd provides the elastictl CLI application
package cmd

import (
	"github.com/urfave/cli/v2"
	"os"
)

// New creates a new CLI application
func New() *cli.App {
	return &cli.App{
		Name:                   "elastictl",
		Usage:                  "Elasticsearch toolkit",
		UsageText:              "elastictl COMMAND [OPTION..] [ARG..]",
		HideHelp:               true,
		HideVersion:            true,
		EnableBashCompletion:   true,
		UseShortOptionHandling: true,
		Reader:                 os.Stdin,
		Writer:                 os.Stdout,
		ErrWriter:              os.Stderr,
		Commands: []*cli.Command{
			cmdExport,
			cmdBlast,
			cmdReshard,
		},
	}
}
