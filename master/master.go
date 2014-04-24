package main

import (
	"flag"
	"fmt"
	. "github.com/JWZH/caskdb/memcache"
	"github.com/robfig/config"
	"log"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

var conf *string = flag.String("conf", "/home/jwzh/goproject/src/github.com/JWZH/caskdb/conf/example.ini", "config path")
var debug *bool = flag.Bool("debug", false, "debug info")
var allocLimit *int = flag.Int("alloc", 1024*4, "cmem alloc limit")

func getServers(serverss string) []string {
	servers := strings.Split(serverss, ",")
	for i := 0; i < len(servers); i++ {
		s := servers[i]
		if p := strings.Index(s, "-"); p > 0 {
			start, _ := strconv.Atoi(s[p-1 : p])
			end, _ := strconv.Atoi(s[p+1:])
			for j := start + 1; j <= end; j++ {
				servers = append(servers, fmt.Sprintf("%s%d", s[:p-1], j))
			}
			s = s[:p]
			servers[i] = s
		}
	}
	sort.Strings(servers)
	return servers
}

func checkServers(client *Client, oldServers []string) {
	c, err := config.ReadDefault(*conf)
	if err == nil {
		serverss, e := c.String("default", "servers")
		if e == nil {
			newServers := getServers(serverss)
			client.UpdateServers(newServers)
		}
	}
	time.Sleep(time.Second * 5)
	go checkServers(client, oldServers)
}

func main() {
	flag.Parse()
	c, err := config.ReadDefault(*conf)
	if err != nil {
		log.Fatal("read config failed", *conf, err.Error())
	}
	if threads, e := c.Int("default", "threads"); e == nil {
		runtime.GOMAXPROCS(threads)
	}

	serverss, e := c.String("default", "servers")
	if e != nil {
		log.Fatal("no servers in conf")
	}
	servers := getServers(serverss)

	AllocLimit = *allocLimit
	if *debug {
		for _, s := range servers {
			log.Println(s)
		}
		AccessLog = log.New(os.Stdout, "", log.Ldate|log.Ltime)
	} else if accesslog, e := c.String("proxy", "accesslog"); e == nil {
		logf, err := os.OpenFile(accesslog, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			log.Print("open " + accesslog + " failed：" + err.Error())
		} else {
			AccessLog = log.New(logf, "", log.Ldate|log.Ltime)
		}
	}
	slow, err := c.Int("proxy", "slow")
	if err != nil {
		slow = 100
	}
	SlowCmdTime = time.Duration(int64(slow) * 1e6)

	schd := NewScheduler(servers)
	client := NewClient(schd)

	http.HandleFunc("/data", func(w http.ResponseWriter, req *http.Request) {
	})

	proxy := NewServer(client)
	listen, e := c.String("proxy", "listen")
	if e != nil {
		listen = "0.0.0.0"
	}
	port, e := c.Int("proxy", "port")
	if e != nil {
		log.Fatal("no proxy port in conf", e.Error())
	}
	addr := fmt.Sprintf("%s:%d", listen, port)
	if e = proxy.Listen(addr); e != nil {
		log.Fatal("proxy listen failed", e.Error())
	}

	log.Println("proxy listen on ", addr)
	go checkServers(client, servers)
	proxy.Serve()
	log.Print("shut down gracefully.")
}
