package main

import (
  "flag"
  "fmt"
  "io"
  "io/ioutil"
  "log"
  "math/rand"
  "net/http"
  "os"
  "strings"
  "time"
)

var dummyDoc = fmt.Sprintf(`{"from":"Phil","to":"Phil","subject":"Hi there!","body":"%s"}`, strings.Repeat("What's up?", 350))
var dummyMapping = `
{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 0
  },
  "mappings": {
    "emails": {
      "properties": {
        "to": {
          "type": "keyword"
        },
        "from": {
          "type": "keyword"
        },
        "subject": {
          "type": "keyword"
        },
        "body": {
          "type": "keyword"
        }
      }
    }
  }
}
`

func main() {
  rand.Seed(time.Now().UnixNano())

  workers := flag.Int("workers", 100, "Number of concurrent workers")
  flag.Parse()

  if flag.NArg() < 1 {
    fmt.Println("Usage: blast-es HOST:PORT/INDEX")
    fmt.Println("  e.g. blast-es localhost:9200/blast-test")
    fmt.Println()

    flag.Usage()
    os.Exit(1)
  }

  rootUri := fmt.Sprintf("http://%s", flag.Arg(0))

  // Create index
  client := &http.Client{}
  req, err := http.NewRequest("PUT", rootUri, strings.NewReader(dummyMapping))
  if err != nil {
    panic(err)
  }

  _, err = client.Do(req)
  if err != nil {
    panic(err)
  }

  // Start workers
  http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = *workers // Avoid opening/closing connections

  docs := make(chan string)
  for i := 0; i < *workers; i++ {
    go func(worker int) {
      blastWorker(worker, client, docs, rootUri)
    }(i)
  }

  for {
    docs <- dummyDoc
  }
}

func blastWorker(worker int, client *http.Client, docs chan string, rootUri string) {
  for line := range docs {
    docId := fmt.Sprintf("%d-%d", worker, time.Now().UnixNano())
    uri := fmt.Sprintf("%s/emails/%s", rootUri, docId)
    req, err := http.NewRequest("PUT", uri, strings.NewReader(line))
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

    log.Printf("[worker %d] resp = %s\n", worker, response.Status)
  }
}
