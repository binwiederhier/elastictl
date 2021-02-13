package cmd

import (
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/urfave/cli/v2"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

var cmdDump = &cli.Command{
	Name:      "dump",
	Aliases:   []string{"d"},
	Usage:     "Dump an entire index to STDOUT",
	UsageText: "elasticblaster dump SERVER INDEX",
	Action:    execDump,
}

func execDump(c *cli.Context) error {
	rand.Seed(time.Now().UnixNano())

	if c.NArg() < 2 {
		return cli.Exit("invalid syntax: ES host and/or index missing", 1)
	}

	index := c.Args().Get(1)
	rootURI := fmt.Sprintf("http://%s/%s", c.Args().Get(0), index)

	client := &http.Client{}

	// Dump mapping first
	req, err := http.NewRequest("GET", rootURI, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	rawMapping, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	mapping := gjson.GetBytes(rawMapping, index).String()
	fmt.Fprintln(c.App.Writer, mapping)

	// Initial search request
	uri := fmt.Sprintf("%s/%s/_search?scroll=1m", rootURI, index)
	req, err = http.NewRequest("POST", uri, strings.NewReader(`{"size":10000}`))
	if err != nil {
		return err
	}
	resp, err = client.Do(req)
	if err != nil {
		return err
	}

	if resp.Body == nil {
		return err
	}

	for {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		scrollId := gjson.GetBytes(body, "_scroll_id")
		if !scrollId.Exists() {
			return cli.Exit("no scroll id", 1)
		}

		hits := gjson.GetBytes(body, "hits.hits")
		if !hits.Exists() {
			panic("no hits")
		}

		if !hits.IsArray() {
			panic("no hits array")
		}

		if len(hits.Array()) == 0 {
			break
		}

		for _, hit := range hits.Array() {
			fmt.Fprintln(c.App.Writer, hit.Raw)
		}

		uri := fmt.Sprintf("%s/_search/scroll", rootURI)
		postBody := fmt.Sprintf(`{"scroll":"1m","scroll_id":"%s"}`, scrollId.String())
		req, err := http.NewRequest("POST", uri, strings.NewReader(postBody))
		if err != nil {
			return err
		}

		resp, err = client.Do(req)
		if err != nil {
			return err
		}

		if resp.Body == nil {
			return err
		}
	}
	return nil
}
