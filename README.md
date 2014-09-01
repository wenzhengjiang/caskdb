# Caskdb

## Introduction

Caskdb is a distributed key-value store inspired by consistent hashing, beansdb and beanseye. I implemented it for learning purpose.

## Features

* automatic data partition
* dynamic node adding
* multiple replication
* data recovery (when restarting after crashed)

## How to Install

Install Go first, then

```
go clone URL(caskdb, bitcask_go)
mv caskdb bitcask_go GOPATH/src/
go get github.com/robfig/config
go install caskdb/master
go install caskdb/datanode

```

## How to run

### master

Prepare configure file(according to caskdb/master/conf/example.ini) and make sure static/ is in current path

```
> master
```

### datanode

```
datanode -port=7901 -dbpath="test1" -debug
datanode -port=7902 -dbpath="test2" -debug
```

### monitor

Open localhost:7908 in browser to monitor the state of datanodes

###  bench

```
cd caskdb/bench
go build
./bench -sz=1K -n=1000 -t=W 
```

## Document

### General Design

Caskdb is designed under the master-slave architecture. Master node is responsible to receiving all requests from clients and send them to data nodes according to its data partition algorithm. It also serves as a cluster monitor which provides a web interface. Data nodes mainly provide an efficient set,get and delete interfaces to outside.

### Data Partition

1. In Caskdb, the mapping relationship between keys and nodes are determined by consistent hashing algorithm.
2. Every key/value pair is stored in two different nodes. 

### Node Adding

1. Modify the configure file(add address of new node).
2. Master node will notice the update of configure file, recalculate the hashing circle and send data migration tasks.
3. Some data nodes will execute the migration tasks.
4. After all tasks are done, the new node is successfully added.

### Key-Value Storage Engine

In Caskdb, the kv engine is [an implementation of Bitcask](https://github.com/JWZH/bitcask_go). The merging operation could be triggered by size of datafile, time window and percentage of useless data. 

```
type Options struct {
	MaxFileSize  int32
	MergeWindow  [2]int // startTime-EndTime
	MergeTrigger float32
	Path         string
}
```

### Configure && Monitor

```
[default]
server_port=7900  # default port
servers=localhost:7901,localhost:7902

[proxy]
port=7905  # proxy port for accessing

[monitor]
port=7908   # monitor port for web 
proxy=localhost:7905   # proxy list to monitor
```
The monitor part is actually stoled from the monitor implementation in [beanseye](https://github.com/douban/beanseye).

