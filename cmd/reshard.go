package cmd

import (
	"errors"
	"fmt"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/urfave/cli/v2"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var cmdReshard = &cli.Command{
	Name:    "reshard",
	Aliases: []string{"c"},
	Action:  execReshard,
}

const rundetailsReindexQueryFormat = `{
  "source": {
    "index": "%s",
    "query": {
      "bool": {
        "filter": {
          "bool": {
            "must_not": [
              {
                "term": {
                  "eventType": "Success"
                }
              }
            ]
          }
        }
      }
    }
  },
  "dest": {
    "index": "%s"
  }
}`

const reindexQueryFormat = `{
  "source": { "index": "%s" },
  "dest": { "index": "%s" }
}`

var client = &http.Client{}

func execReshard(c *cli.Context) error {
	var err error
	index := c.Args().Get(0)
	if index == "" {
		return errors.New("expected target index name, got nothing")
	}
	tempIndex := fmt.Sprintf("%s-temp", index)

	mapping, err := readMapping(index)
	if err != nil {
		return err
	}
	newMapping, err := updateMapping(index, mapping)
	if err != nil {
		return err
	}

	// Create temporary index and reindex into it
	if err := createIndex(tempIndex, newMapping); err != nil {
		return err
	}
	filteredRundetailsReindexQuery := fmt.Sprintf(rundetailsReindexQueryFormat, index, tempIndex)
	taskID, err := reindex(index, tempIndex, filteredRundetailsReindexQuery)
	if err != nil {
		return err
	}
	if err := waitForTask(taskID); err != nil {
		return err
	}

	// Delete original index
	if err := deleteIndex(index); err != nil {
		return err
	}

	// Copy/reindex back to original index
	if err := createIndex(index, newMapping); err != nil {
		return err
	}
	reindexQuery := fmt.Sprintf(reindexQueryFormat, tempIndex, index)
	taskID, err = reindex(tempIndex, index, reindexQuery)
	if err != nil {
		return err
	}
	if err := waitForTask(taskID); err != nil {
		return err
	}

	// Delete temp index
	if err := deleteIndex(tempIndex); err != nil {
		return err
	}

	return nil
}

func readMapping(index string) (string, error) {
	log.Printf("reading mapping for index %s", index)

	rootIndexURI := fmt.Sprintf("http://localhost:9200/%s", index)
	req, err := http.NewRequest("GET", rootIndexURI, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	rawMapping, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	mapping := gjson.GetBytes(rawMapping, index).String()
	mappingFile := fmt.Sprintf("%s.mapping", index)
	if err := os.WriteFile(mappingFile, []byte(mapping), 0644); err != nil {
		return "", err
	}
	log.Printf("saved mapping to %s", mappingFile)
	return mapping, nil
}

func updateMapping(index, mapping string) (string, error) {
	log.Printf("updating mapping to match desired shard/replica scheme")

	var err error
	for _, keyToRemove := range settingsToRemove {
		mapping, err = sjson.Delete(mapping, keyToRemove)
		if err != nil {
			return "", err
		}
	}
	mapping, err = sjson.Set(mapping, "settings.index.number_of_replicas", "1")
	if err != nil {
		return "", err
	}
	mapping, err = sjson.Set(mapping, "settings.index.number_of_shards", "1")
	if err != nil {
		return "", err
	}
	mappingFile := fmt.Sprintf("%s.new-mapping", index)
	if err := os.WriteFile(mappingFile, []byte(mapping), 0644); err != nil {
		return "", err
	}
	log.Printf("saved new mapping to %s", mappingFile)

	return mapping, nil
}

func createIndex(index string, mapping string) error {
	log.Printf("creating index %s", index)

	rootIndexURI := fmt.Sprintf("http://localhost:9200/%s", index)
	req, err := http.NewRequest("PUT", rootIndexURI, strings.NewReader(mapping))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode != 201 && resp.StatusCode != 200 {
		responseBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected response code during index creation: %d %s", resp.StatusCode, string(responseBody))
	}
	return nil
}

// reindex kicks off the reindex process and returns a task ID if successful
func reindex(fromIndex string, toIndex string, query string) (string, error) {
	log.Printf("reindexing index %s into index %s", fromIndex, toIndex)

	reindexURI := fmt.Sprintf("http://localhost:9200/_reindex?wait_for_completion=false")
	req, err := http.NewRequest("POST", reindexURI, strings.NewReader(query))
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != 201 && resp.StatusCode != 200 {
		return "", fmt.Errorf("unexpected response code during reindex call: %d %s\n", resp.StatusCode, string(responseBody))
	}
	task := gjson.GetBytes(responseBody, "task")
	if !task.Exists() {
		return "", fmt.Errorf("unexpected response, no task ID found: %s\n", string(responseBody))
	}
	log.Printf("reindexing queued in ES as task %s", task.String())
	return task.String(), nil
}

func waitForTask(taskID string) error {
	log.Printf("waiting for task completion")
	for {
		time.Sleep(time.Second)

		taskRequestURI := fmt.Sprintf("http://localhost:9200/_tasks/%s", taskID)
		resp, err := client.Get(taskRequestURI)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		responseBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if resp.StatusCode != 201 && resp.StatusCode != 200 {
			return fmt.Errorf("unexpected response code during task call: %d %s\n", resp.StatusCode, string(responseBody))
		}

		// {
		//  "completed": false,
		//  "task": {
		//    "status": {
		//      "total": 460205,
		//      "created": 162000,
		//      ...
		//    },
		// ...

		completed := gjson.GetBytes(responseBody, "completed")
		total := gjson.GetBytes(responseBody, "task.status.total")
		created := gjson.GetBytes(responseBody, "task.status.created")
		if !completed.Exists() || !total.Exists() || !created.Exists() {
			return fmt.Errorf("unexpected response for task status call: %s\n", string(responseBody))
		}
		if completed.Bool() {
			fmt.Print("\r")
			log.Printf("reindexing complete: %d/%d document(s) created", created.Int(), total.Int())
			break
		}
		percent := 100.0
		if total.Int() > 0 { // div by zero!
			percent = float64(created.Int())/float64(total.Int())*100
		}
		fmt.Printf("\r%d/%d document(s) created, %.2f%%   ", created.Int(), total.Int(), percent)
	}

	return nil
}

func deleteIndex(index string) error {
	log.Printf("deleting index %s", index)

	indexURI := fmt.Sprintf("http://localhost:9200/%s", index)
	req, err := http.NewRequest("DELETE", indexURI, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 201 && resp.StatusCode != 200 {
		return fmt.Errorf("unexpected response code during delete call: %d %s\n", resp.StatusCode, string(responseBody))
	}
	return nil
}
