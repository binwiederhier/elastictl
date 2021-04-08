# elastictl

Simple tool to dump an elasticsearch index into a file to later insert it wih high concurrency. 
This can be used for load testing. In my local cluster, I was able to index ~10k documents per second.

Usage:   
```
# Build
go build 

# Dump index
./elastictl dump localhost:9200 dummy > dummy.json

# Insert index with high concurrency
cat dummy.json | ./elastictl blast -workers 100 localhost:9200 dummy-copy
```
