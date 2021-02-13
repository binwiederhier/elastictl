package main

import (
  "flag"
  "fmt"
  "github.com/tidwall/gjson"
  "io/ioutil"
  "math/rand"
  "net/http"
  "os"
  "strings"
  "time"
)

func main() {
  rand.Seed(time.Now().UnixNano())
  flag.Parse()

  if flag.NArg() < 2 {
    fmt.Println("Usage: dump-es HOST:PORT INDEX")
    fmt.Println("  e.g. dump-es localhost:9200 test")
    fmt.Println()

    flag.Usage()
    os.Exit(1)
  }

  rootUri := fmt.Sprintf("http://%s", flag.Arg(0))
  index := flag.Arg(1)

  // Initial request

  client := &http.Client{}

  uri := fmt.Sprintf("%s/%s/_search?scroll=1m", rootUri, index)
  postBody := `{"size":10000}`
  req, err := http.NewRequest("POST", uri, strings.NewReader(postBody))
  if err != nil {
    panic(err)
  }

  response, err := client.Do(req)
  if err != nil {
    panic(err)
  }

  if response.Body == nil {
    panic(err)
  }

  for {
    body, err := ioutil.ReadAll(response.Body)
    if err != nil {
      panic(err)
    }

    scrollId := gjson.GetBytes(body, "_scroll_id")
    if !scrollId.Exists() {
      panic("no scroll id")
    }

    hits := gjson.GetBytes(body, "hits.hits")
    if !hits.Exists() {
      panic("no hits")
    }

    if !hits.IsArray() {
      panic("no hits array")
    }

    if len(hits.Array()) == 0 {
      fmt.Fprintln(os.Stderr, "done")
      break
    }

    for _, hit := range hits.Array() {
      fmt.Println(hit.Raw)
    }

    uri := fmt.Sprintf("%s/_search/scroll", rootUri)
    postBody := fmt.Sprintf(`{"scroll":"1m","scroll_id":"%s"}`, scrollId.String())
    req, err := http.NewRequest("POST", uri, strings.NewReader(postBody))
    if err != nil {
      panic(err)
    }

    response, err = client.Do(req)
    if err != nil {
      panic(err)
    }

    if response.Body == nil {
      panic(err)
    }
  }

}
