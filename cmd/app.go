// Package cmd provides the elasticblaster CLI application
package cmd

import (
	"github.com/urfave/cli/v2"
	"os"
)

// New creates a new CLI application
func New() *cli.App {
	return &cli.App{
		Name:                   "elasticblaster",
		Usage:                  "copy/paste across machines",
		UsageText:              "elasticblaster COMMAND [OPTION..] [ARG..]",
		HideHelp:               true,
		HideVersion:            true,
		EnableBashCompletion:   true,
		UseShortOptionHandling: true,
		Reader:                 os.Stdin,
		Writer:                 os.Stdout,
		ErrWriter:              os.Stderr,
		Commands: []*cli.Command{
			cmdDump,
			cmdBlast,
		},
	}
}
