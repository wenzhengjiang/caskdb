# Caskdb

## Introduction

Caskdb is a distributed key-value store inspired by consistent hashing, beansdb and beanseye. I implemented it for learning purpose. Details about Caskdb is [here](https://jwzh.github.io/2014/05/30/caskdb---a-simple-distributed-key-value-store/)

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

* master
Prepare configure file(according to caskdb/master/conf/example.ini) and make sure static/ is in current path

```
> master
```

* datanode

```
datanode -port=7901 -dbpath="test1" -debug
datanode -port=7902 -dbpath="test2" -debug
```

* monitor
Open localhost:7908 in browser to monitor the state of datanodes

* bench

```
cd caskdb/bench
go build
./bench -sz=1K -n=1000 -t=W 
```
