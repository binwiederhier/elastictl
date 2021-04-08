# elastictl

Simple tool to dump an elasticsearch index into a file to later insert it wih high concurrency. 
This can be used for load testing. In my local cluster, I was able to index ~10k documents per second.

Usage:   
```
# Build
go build 

# Dump index
elastictl export dummy > dummy.json

# Insert index with high concurrency
cat dummy.json | elastictl import --workers 100 dummy-copy

# Reshard (import/export) an index
elastictl reshard \
  --search '{"query":{"bool":{"must_not":{"match":{"eventType":"Success"}}}}}' \
  --shards 1 \
  --replicas 1 \
  dummy-index
```
