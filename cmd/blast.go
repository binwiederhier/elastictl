package cmd

import (
	"bufio"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/urfave/cli/v2"
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
	UsageText: "elasticblaster blast SERVER INDEX",
	Action:    execBlast,
	Flags: []cli.Flag{
		&cli.IntFlag{Name: "workers", Aliases: []string{"w"}, Value: 30, Usage: "number of concurrent workers"},
		&cli.BoolFlag{Name: "nocreate", Aliases: []string{"N"}, Value: false, Usage: "do not create index"},
	},
}

var spinner = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

type progressBar struct {
	started time.Time
	count int
	size int64
	rendered time.Time
	rendercount int64
	prevlen int
	mu sync.Mutex
}

func newProgressBar() *progressBar {
	return &progressBar{
		started: time.Now(),
	}
}

func (p *progressBar) Add(size int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.count++
	p.size += size
	if time.Since(p.rendered) > 65 * time.Millisecond {
		p.render(false)
	}
}

func (p *progressBar) Done() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.render(true)
}

func (p *progressBar) render(done bool) {
	spin := spinner[p.rendercount%int64(len(spinner))]
	count := p.count
	countPerSec := float64(p.count) / time.Since(p.started).Seconds()
	size := bytesToHuman(p.size)
	sizePerSec := bytesToHuman(int64(float64(p.size) / time.Since(p.started).Seconds()))
	if done {
		line := fmt.Sprintf("\rindexing finished: %d docs (%.2f docs/s), %s (%s/s)", count, countPerSec, size, sizePerSec)
		fmt.Print(line)
		if p.prevlen > len(line) {
			fmt.Print(strings.Repeat(" ", p.prevlen-len(line)))
		}
		fmt.Println()
		p.prevlen = len(line)
	} else {
		line := fmt.Sprintf("\r%s indexing: %d docs (%.2f docs/s), %s (%s/s)", spin, count, countPerSec, size, sizePerSec)
		fmt.Print(line)
		if p.prevlen > len(line) {
			fmt.Print(strings.Repeat(" ", p.prevlen-len(line)))
		}
		p.prevlen = len(line)
	}
	p.rendered = time.Now()
	p.rendercount++
}

func execBlast(c *cli.Context) error {
	rand.Seed(time.Now().UnixNano())

	workers := c.Int("workers")
	nocreate := c.Bool("nocreate")

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
	if !nocreate {
		mapping := gjson.Get(rawMapping, index)

		req, err := http.NewRequest("PUT", rootURI, strings.NewReader(mapping.String()))
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
	progress := newProgressBar()
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

func blastWorker(wg *sync.WaitGroup, docsChan chan string, progress *progressBar, client *http.Client, rootURI string) {
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

func bytesToHuman(b int64) string {
	// From: https://yourbasic.org/golang/formatting-byte-size-to-human-readable-format/
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

