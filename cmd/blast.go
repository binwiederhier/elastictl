package cmd

import (
	"bufio"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/urfave/cli/v2"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var cmdBlast = &cli.Command{
	Name:      "blast",
	Aliases:   []string{"b"},
	Usage:     "Write to ES index, either from STDIN",
	UsageText: "elasticblaster blast SERVER INDEX",
	Action:    execBlast,
	Flags: []cli.Flag{
		&cli.IntFlag{Name: "workers", Aliases: []string{"w"}, Value: 30, Usage: "number of concurrent workers"},
	},
}

func execBlast(c *cli.Context) error {
	rand.Seed(time.Now().UnixNano())
	workers := c.Int("workers")

	if c.NArg() < 2 {
		return cli.Exit("invalid syntax, need ES hostname/port and index", 1)
	}

	index := c.Args().Get(1)
	rootURI := fmt.Sprintf("http://%s/%s", c.Args().Get(0), index)

	client := &http.Client{}
	scanner := bufio.NewScanner(c.App.Reader)

	// Create index
	if !scanner.Scan() {
		return cli.Exit("cannot read mapping from STDIN", 1)
	}

	rawMapping := scanner.Text()
	mapping := gjson.Get(rawMapping, index)

	req, err := http.NewRequest("PUT", rootURI, strings.NewReader(mapping.String()))
	if err != nil {
		return err
	}
	_, err = client.Do(req)
	if err != nil {
		return err
	}

	// Start workers
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = workers // Avoid opening/closing connections

	wg := &sync.WaitGroup{}
	docsChan := make(chan string)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go blastWorker(wg, docsChan, client, rootURI)
	}

	for scanner.Scan() {
		docsChan <- scanner.Text()
	}

	close(docsChan)
	wg.Wait()

	return nil
}

func blastWorker(wg *sync.WaitGroup, docsChan chan string, client *http.Client, rootURI string) {
	defer wg.Done()
	for doc := range docsChan {
		id := url.QueryEscape(gjson.Get(doc, "_id").String())
		dtype := gjson.Get(doc, "_type").String()
		source := gjson.Get(doc, "_source").String()
		uri := fmt.Sprintf("%s/%s/%s", rootURI, dtype, id)
		req, err := http.NewRequest("PUT", uri, strings.NewReader(source))
		if err != nil {
			panic(err)
		}

		response, err := client.Do(req)
		if err != nil {
			panic(err)
		}

		if response.Body != nil {
			io.Copy(ioutil.Discard, response.Body)
			response.Body.Close()
		}

		log.Printf("resp = %s\n", response.Status)
	}
}

