package tools

import (
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/urfave/cli/v2"
	"io"
	"io/ioutil"
	"log"
	"net/http"
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

	for {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		scrollID := gjson.GetBytes(body, "_scroll_id")
		if !scrollID.Exists() {
			return cli.Exit("no scroll id: "+string(body), 1)
		}

		hits := gjson.GetBytes(body, "hits.hits")
		if !hits.Exists() {
			return errors.New("no hits")
		}

		if !hits.IsArray() {
			return errors.New("no hits array")
		}

		if len(hits.Array()) == 0 {
			break
		}

		for _, hit := range hits.Array() {
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
	log.Printf("export complete")
	return nil
}
