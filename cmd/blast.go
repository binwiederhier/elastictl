package cmd

import (
	"bufio"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/urfave/cli/v2"
	"heckel.io/elastictl/util"
	"io"
	"io/ioutil"
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
	UsageText: "elastictl blast SERVER INDEX",
	Action:    execBlast,
	Flags: []cli.Flag{
		&cli.IntFlag{Name: "workers", Aliases: []string{"w"}, Value: 100, Usage: "number of concurrent workers"},
		&cli.IntFlag{Name: "shards", Aliases: []string{"s"}, Value: 0, Usage: "override the number of shards on index creation"},
		&cli.IntFlag{Name: "replicas", Aliases: []string{"r"}, Value: 0, Usage: "override the number of replicas on index creation"},
		&cli.BoolFlag{Name: "nocreate", Aliases: []string{"N"}, Value: false, Usage: "do not create index"},
	},
}

func execBlast(c *cli.Context) error {
	rand.Seed(time.Now().UnixNano())

	workers := c.Int("workers")
	nocreate := c.Bool("nocreate")
	shards := c.Int("shards")
	replicas := c.Int("replicas")

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
	mapping := scanner.Text()
	if !nocreate {
		var err error
		if shards > 0 {
			mapping, err = sjson.Set(mapping, "settings.index.number_of_replicas", fmt.Sprintf("%d", shards))
			if err != nil {
				return err
			}
		}
		if replicas > 0 {
			mapping, err = sjson.Set(mapping, "settings.index.number_of_shards", fmt.Sprintf("%d", replicas))
			if err != nil {
				return err
			}
		}
		req, err := http.NewRequest("PUT", rootURI, strings.NewReader(mapping))
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != 201 && resp.StatusCode != 200 {
			return cli.Exit(fmt.Sprintf("unexpected response code during index creation: %d", resp.StatusCode), 1)
		}
	}

	// Start workers
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = workers // Avoid opening/closing connections

	wg := &sync.WaitGroup{}
	docsChan := make(chan string)
	progress := util.NewProgressBar()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go blastWorker(wg, docsChan, progress, client, rootURI)
	}

	go func() {
		for scanner.Scan() {
			docsChan <- scanner.Text()
		}
		close(docsChan)
	}()

	wg.Wait()
	progress.Done()

	return nil
}

func blastWorker(wg *sync.WaitGroup, docsChan chan string, progress *util.ProgressBar, client *http.Client, rootURI string) {
	defer wg.Done()
	for doc := range docsChan {
		id := url.QueryEscape(gjson.Get(doc, "_id").String())
		dtype := gjson.Get(doc, "_type").String()
		source := gjson.Get(doc, "_source").String()
		uri := fmt.Sprintf("%s/%s/%s", rootURI, dtype, id)
		req, err := http.NewRequest("PUT", uri, strings.NewReader(source))
		if err != nil {
			fmt.Printf("error creating PUT request: %s\n", err.Error())
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("PUT failed: %s\n", err.Error())
			continue
		}
		if resp.StatusCode != 201 && resp.StatusCode != 200 {
			fmt.Printf("PUT returned unexpected response: %d\n", resp.StatusCode)
			continue
		}
		if resp.Body != nil {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}
		progress.Add(int64(len(source)))
	}
}
