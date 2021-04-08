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

func Export(host string, index string, search string, w io.Writer) error {
	log.Printf("exporting index %s/%s", host, index)
	rootURI := fmt.Sprintf("http://%s", host)

	// Dump mapping first
	rootIndexURI := fmt.Sprintf("http://%s/%s", host, index)
	req, err := http.NewRequest("GET", rootIndexURI, nil)
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
	if _, err := fmt.Fprintln(w, mapping); err != nil {
		return err
	}

	// Initial search request
	var body io.Reader
	if search != "" {
		body = strings.NewReader(search)
	}
	uri := fmt.Sprintf("%s/_search?size=10000&scroll=1m", rootIndexURI)
	req, err = http.NewRequest("POST", uri, body)
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

	total := int64(-1)
	progress := util.NewProgressBar(os.Stderr)

	for {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		if total == -1 {
			t := gjson.GetBytes(body, "hits.total")
			if !t.Exists() {
				return errors.New("no total")
			}
			total = t.Int()
		}

		scrollID := gjson.GetBytes(body, "_scroll_id")
		if !scrollID.Exists() {
			return errors.New("no scroll id: "+string(body))
		}

		hits := gjson.GetBytes(body, "hits.hits")
		if !hits.Exists() || !hits.IsArray() {
			return errors.New("no hits: "+string(body))
		}
		if len(hits.Array()) == 0 {
			break // we're done!
		}

		for _, hit := range hits.Array() {
			progress.Add(1)
			if _, err := fmt.Fprintln(w, hit.Raw); err != nil {
				return err
			}
		}

		uri := fmt.Sprintf("%s/_search/scroll", rootURI)
		postBody := fmt.Sprintf(`{"scroll":"1m","scroll_id":"%s"}`, scrollID.String())
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
	progress.Done()
	return nil
}
