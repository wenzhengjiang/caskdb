package main

import (
	. "bitcask_go"
	"caskdb/protocol"
	"flag"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"runtime"
	"strings"
	"time"
)

type BitcaskStore struct {
	bc  *Bitcask
	chF chan func()
}

func crc32hash(s []byte) uint32 {
	hash := crc32.NewIEEE()
	hash.Write(s)
	return hash.Sum32()
}

func NewStore(c Config) *BitcaskStore {
	b := new(BitcaskStore)
	b.bc = new(Bitcask)
	b.chF = make(chan func(), 100)
	go b.backend()
	var err error
	b.bc, err = NewBitcask(c.Options)
	if err != nil {
		panic("Can not open db:" + c.Path + err.Error())
	}
	return b
}

func (self *BitcaskStore) backend() {
	for f := range self.chF {
		f()
	}

}
func (self *BitcaskStore) Close() error {
	if err := self.bc.Close(); err != nil {
		return err
	}
	return nil
}

func (self *BitcaskStore) migrate(host string, left, right uint32) {
	keyChan := self.bc.Keys()
	target := protocol.NewHost(host)
	for key := range keyChan {
		v := crc32hash([]byte(key))
		if (left < right && v >= left && v < right) ||
			(left > right && !(v >= left && v < right)) {
			v, e := self.bc.Get(key)
			if e == nil {
				target.Set(key, &protocol.Item{Body: v}, false)
			}
		}
	}
}

func (self *BitcaskStore) FlushAll() {
	self.bc.Sync()
}

func (self *BitcaskStore) Get(key string) (*protocol.Item, error) {
	self.bc.Sync()
	if len(key) > 3 && key[0:3] == "@#$" {
		var addr string
		var left, right uint32
		fmt.Sscanf("%s-%d-%d", addr, &left, &right)
		go self.migrate(addr, left, right)
		return &protocol.Item{Body: []byte("TRUST ME")}, nil
	}
	v, err := self.bc.Get(key)
	if err != nil {
		return nil, err
	}
	return &protocol.Item{Body: v}, nil
}

func (self *BitcaskStore) Set(key string, item *protocol.Item, noreply bool) (bool, error) {
	if len(key) > 3 && strings.Contains(key, "@#$") {
		pos := strings.Index(key, "@#$")
		target := protocol.NewHost(key[pos+3:])
		key = key[:pos]
		self.chF <- func() {
			for {
				if ok, _ := target.Set(key, item, noreply); ok {
					break
				}
			}
		}
	}
	e := self.bc.Set(key, item.Body)
	if e != nil {
		return false, e
	}
	return true, nil
}

func (self *BitcaskStore) Len() int64 {
	return self.bc.Len()
}

func (self *BitcaskStore) Delete(key string) (bool, error) {
	e := self.bc.Del(key)
	if e != nil {
		return false, e
	} else {
		return true, nil
	}
}

var listen *string = flag.String("listen", "0.0.0.0", "address to listen")
var port *int = flag.Int("port", 7901, "port to listen")
var accesslog *string = flag.String("accesslog", "", "access log path")
var debug *bool = flag.Bool("debug", false, "debug info")
var threads *int = flag.Int("threads", 8, "number of threads")
var memlimit *int = flag.Int("memlimit", 1024*2, "limit memory used by go heap (M)")

var dbpath *string = flag.String("dbpath", "testdb", "config path")
var dbmaxFileSize *int = flag.Int("fsz", 1024*1024*1024, "max file size")
var dbMergeWindow *string = flag.String("window", "00_23", "bitcask merge window")
var dbMergeTrigger *float64 = flag.Float64("trigger", 0.6, "bitcask merge trigger")

type Config struct {
	Options
}

func main() {
	flag.Parse()

	runtime.GOMAXPROCS(*threads)

	// config log
	if *accesslog != "" {
		logf, err := os.OpenFile(*accesslog, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Print("open " + *accesslog + " failed")
			return
		}
		protocol.AccessLog = log.New(logf, "", log.Ldate|log.Ltime)
	} else if *debug {
		protocol.AccessLog = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	}
	// config store
	var st, et int
	fmt.Sscanf(*dbMergeWindow, "%d_%d", &st, &et)
	storeConf := Config{Options{
		Path:         *dbpath,
		MaxFileSize:  int32(*dbmaxFileSize),
		MergeWindow:  [2]int{st, et},
		MergeTrigger: float32(*dbMergeTrigger),
	}}
	store := NewStore(storeConf)
	defer store.Close()

	// config server
	addr := fmt.Sprintf("%s:%d", *listen, *port)
	s := protocol.NewServer(store)
	e := s.Listen(addr)
	if e != nil {
		log.Print("Listen at", *listen, "failed")
		return
	}
	// monitor mem usage
	go func() {
		ul := uint64(*memlimit) * 1024 * 1024
		memStats := &runtime.MemStats{}
		runtime.ReadMemStats(memStats)
		for memStats.HeapSys < ul {
			time.Sleep(1e9)
		}
		log.Print("Mem used by Go is over limitation ", memStats.HeapSys/1024/1024, *memlimit)
		s.Shutdown()
	}()
	s.Serve()
	log.Print("shut down gracefully.")
}
