# elastictl

Simple tool import/export Elasticsearch indices into a file, and/or reshard an index. The tool can be used for:

* Backup/restore of an Elasticsearch index 
* Performance test an Elasticsearch cluster (import with high concurrency, see `--workers`)
* Change the shard/replica count of an index (see `reshard` subcomment)

In my local cluster, I was able to import ~10k documents per second.

## Build
```
$ go build
```

Or via goreleaser:
```
$ make [build | build-snapshot]
```

## Usage:

### Export/dump an index to a file
The first line of the output format is the mapping, the rest are the documents.
```
# Entire index
elastictl export dummy > dummy.json

# Only a subset of documents
elastictl export \
  --search '{"query":{"bool":{"must_not":{"match":{"eventType":"Success"}}}}}' \
  dummy > dummy.json
```

### Import to new index
```
# With high concurrency
cat dummy.json | elastictl import --workers 100 dummy-copy
```

### Reshard (import/export) an index
This commands export the index `dummy` to `dummy.json` and re-imports it as `dummy` using a different number of shards.
This command does `DELETE` the index after exporting it!
```
elastictl reshard \
  --search '{"query":{"bool":{"must_not":{"match":{"eventType":"Success"}}}}}' \
  --shards 1 \
  --replicas 1 \
  dummy
```
