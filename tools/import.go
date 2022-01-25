package tools

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"heckel.io/elastictl/util"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	client              = &http.Client{}
	settingsToRemove    = []string{"settings.index.creation_date", "settings.index.uuid", "settings.index.version", "settings.index.provided_name"}
	errTemporaryFailure = errors.New("temporary failure")
)

func Import(host string, index string, workers int, nocreate bool, shards int, replicas int, r io.Reader, totalHint int) (int, error) {
	log.Printf("importing index %s/%s", host, index)
	rootURI := fmt.Sprintf("http://%s/%s", host, index)
	scanner := bufio.NewScanner(r)

	// Create index
	if !scanner.Scan() {
		return 0, errors.New("cannot read mapping")
	}
	mapping := scanner.Text()
	if !nocreate {
		var err error
		for _, keyToRemove := range settingsToRemove {
			mapping, err = sjson.Delete(mapping, keyToRemove)
			if err != nil {
				return 0, err
			}
		}
		if shards > 0 {
			mapping, err = sjson.Set(mapping, "settings.index.number_of_shards", fmt.Sprintf("%d", shards))
			if err != nil {
				return 0, err
			}
		}
		if replicas > -1 { // zero replicas is allowed!
			mapping, err = sjson.Set(mapping, "settings.index.number_of_replicas", fmt.Sprintf("%d", replicas))
			if err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequest("PUT", rootURI, strings.NewReader(mapping))
        req.Header.Add("Content-Type", "application/json")
		if err != nil {
			return 0, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		if resp.StatusCode == 400 || resp.StatusCode == 503 {
			return 0, errTemporaryFailure // special case: 400 returned when index already exists, 503 when the cluster is overloaded
		} else if resp.StatusCode != 201 && resp.StatusCode != 200 {
			return 0, fmt.Errorf("unexpected response code during index creation: %d", resp.StatusCode)
		}
	}

	// Start workers
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = workers // Avoid opening/closing connections

	wg := &sync.WaitGroup{}
	docsChan := make(chan string)
	progress := util.NewProgressBarWithTotal(os.Stderr, totalHint)
	imported := int64(0)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go importWorker(wg, docsChan, progress, client, rootURI, &imported)
	}

	go func() {
		for scanner.Scan() {
			docsChan <- scanner.Text()
		}
		close(docsChan)
	}()

	wg.Wait()
	progress.Done()

	return int(imported), nil
}

func importWorker(wg *sync.WaitGroup, docsChan chan string, progress *util.ProgressBar, client *http.Client, rootURI string, imported *int64) {
	defer wg.Done()
	for doc := range docsChan {
		id := url.QueryEscape(gjson.Get(doc, "_id").String())
		dtype := gjson.Get(doc, "_type").String()
		source := gjson.Get(doc, "_source").String()
		uri := fmt.Sprintf("%s/%s/%s", rootURI, dtype, id)
		req, err := http.NewRequest("PUT", uri, strings.NewReader(source))
        req.Header.Add("Content-Type", "application/json")
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
		atomic.AddInt64(imported, 1)
	}
}
