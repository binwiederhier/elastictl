package tools

import (
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"heckel.io/elastictl/util"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

func Export(host string, index string, search string, w io.Writer) (int, error) {
	log.Printf("exporting index %s/%s", host, index)
	rootURI := fmt.Sprintf("http://%s", host)

	// Dump mapping first
	rootIndexURI := fmt.Sprintf("http://%s/%s", host, index)
	req, err := http.NewRequest("GET", rootIndexURI, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	rawMapping, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	mapping := gjson.GetBytes(rawMapping, index).String()
	if _, err := fmt.Fprintln(w, mapping); err != nil {
		return 0, err
	}

	// Initial search request
	var body io.Reader
	if search != "" {
		body = strings.NewReader(search)
	}
	uri := fmt.Sprintf("%s/_search?size=10000&scroll=1m", rootIndexURI)
	req, err = http.NewRequest("POST", uri, body)
	if err != nil {
		return 0, err
	}
	resp, err = client.Do(req)
	if err != nil {
		return 0, err
	}
	if resp.Body == nil {
		return 0, err
	}

	var progress *util.ProgressBar
	exported := 0

	for {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return 0, err
		}

		if progress == nil {
			total := gjson.GetBytes(body, "hits.total")
			if !total.Exists() {
				return 0, errors.New("no total")
			}
			progress = util.NewProgressBarWithTotal(os.Stderr, int(total.Int()))
		}

		scrollID := gjson.GetBytes(body, "_scroll_id")
		if !scrollID.Exists() {
			return 0, errors.New("no scroll id: " + string(body))
		}

		hits := gjson.GetBytes(body, "hits.hits")
		if !hits.Exists() || !hits.IsArray() {
			return 0, errors.New("no hits: " + string(body))
		}
		if len(hits.Array()) == 0 {
			break // we're done!
		}

		for _, hit := range hits.Array() {
			exported++
			progress.Add(int64(len(hit.Raw)))
			if _, err := fmt.Fprintln(w, hit.Raw); err != nil {
				return 0, err
			}
		}

		uri := fmt.Sprintf("%s/_search/scroll", rootURI)
		postBody := fmt.Sprintf(`{"scroll":"1m","scroll_id":"%s"}`, scrollID.String())
		req, err := http.NewRequest("POST", uri, strings.NewReader(postBody))
		if err != nil {
			return 0, err
		}

		resp, err = client.Do(req)
		if err != nil {
			return 0, err
		}

		if resp.Body == nil {
			return 0, err
		}
	}
	progress.Done()
	return exported, nil
}
