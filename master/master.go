package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	. "github.com/JWZH/caskdb/memcache"
	"github.com/robfig/config"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"text/template"
	"time"
)

var conf *string = flag.String("conf", "/home/jwzh/goproject/src/github.com/JWZH/caskdb/conf/example.ini", "config path")
var debug *bool = flag.Bool("debug", true, "debug info")
var allocLimit *int = flag.Int("alloc", 1024*4, "cmem alloc limit")

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func makeGzipHandler(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			fn(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		fn(gzipResponseWriter{Writer: gz, ResponseWriter: w}, r)
	}
}

func in(s, subs interface{}) (bool, error) {
	return strings.Contains(s.(string), subs.(string)), nil
}

func timer(v interface{}) string {
	if v == nil {
		return ""
	}
	t := v.(uint64)
	switch {
	case t > 3600*24*2:
		return fmt.Sprintf("%d day", t/3600/24)
	case t > 3600*2:
		return fmt.Sprintf("%d hour", t/3600)
	case t > 60*2:
		return fmt.Sprintf("%d min", t/60)
	default:
		return fmt.Sprintf("%d sec", t)
	}
	return ""
}

func sum(l interface{}) uint64 {
	if li, ok := l.([]uint64); ok {
		s := uint64(0)
		for _, n := range li {
			s += n
		}
		return s
	}
	return 0
}

func sizer(v interface{}) string {
	var n float64
	switch i := v.(type) {
	case int:
		n = float64(i)
	case uint:
		n = float64(i)
	case int64:
		n = float64(i)
	case uint64:
		n = float64(i)
	case float64:
		n = float64(i)
	case float32:
		n = float64(i)
	default:
		return "0"
	}
	if math.IsInf(n, 0) {
		return "Inf"
	}
	unit := 0
	var units = []string{"", "K", "M", "G", "T", "P"}
	for n > 1024.0 {
		n /= 1024
		unit += 1
	}
	s := fmt.Sprintf("%2.1f", n)
	if strings.HasSuffix(s, ".0") || len(s) >= 4 {
		s = s[:len(s)-2]
	}
	return s + units[unit]
}

func number(v interface{}) string {
	var n float64
	switch i := v.(type) {
	case int:
		n = float64(i)
	case uint:
		n = float64(i)
	case int64:
		n = float64(i)
	case uint64:
		n = float64(i)
	case float64:
		n = float64(i)
	case float32:
		n = float64(i)
	default:
		return "0"
	}
	if math.IsInf(n, 0) {
		return "Inf"
	}
	unit := 0
	var units = []string{"", "k", "m", "b"}
	for n > 1000.0 {
		n /= 1000
		unit += 1
	}
	s := fmt.Sprintf("%2.1f", n)
	if strings.HasSuffix(s, ".0") || len(s) >= 4 {
		s = s[:len(s)-2]
	}
	return s + units[unit]
}

var tmpls *template.Template
var SECTIONS = [][]string{{"SS", "Server"}}

var server_stats []map[string]interface{}
var proxy_stats []map[string]interface{}
var total_records, uniq_records uint64
var bucket_stats []string
var schd Scheduler
var client *Client

func update_stats(servers []string, hosts []*Host, server_stats []map[string]interface{}, isNode bool) {
	if AccessLog != nil {
		AccessLog.Println(servers)
	}
	if hosts == nil {
		hosts = make([]*Host, len(servers))
		for i, s := range servers {
			hosts[i] = NewHost(s)
		}
	}
	// call self after 10 seconds
	time.AfterFunc(time.Second*10, func() {
		checkServers(client, servers)
	})

	defer func() {
		if err := recover(); err != nil {
			log.Print("update stats failed", err)
		}
	}()
	for i, h := range hosts {
		t, err := h.Stat()
		if err != nil {
			server_stats[i] = map[string]interface{}{"name": h.Addr}
			continue
		}

		st := make(map[string]interface{})
		st["name"] = h.Addr
		//log.Print(h.Addr, t)
		for k, v := range t {
			switch k {
			case "version", "pid":
				st[k] = v
			case "rusage_maxrss":
				if n, e := strconv.ParseInt(v, 10, 64); e == nil {
					st[k] = uint64(n) * 1024
				}
			default:
				var e error
				st[k], e = strconv.ParseUint(v, 10, 64)
				if e != nil {
					println("conv to ui64 failed", v)
					st[k] = 0
				}
			}
		}

		ST := func(name string) uint64 {
			if v, ok := st[name]; ok && v != nil {
				return v.(uint64)
			}
			return 0
		}

		st["hit"] = ST("get_hits") * 100 / (ST("cmd_get") + 1)
		st["getset"] = float32(ST("cmd_get")) / float32(ST("cmd_set")+100.0)
		if maxrss, ok := st["rusage_maxrss"]; ok {
			st["mpr"] = maxrss.(uint64) / (st["total_items"].(uint64) + st["curr_items"].(uint64) + 1000)
		}
		old := server_stats[i]
		keys := []string{"cmd_get", "cmd_set", "cmd_delete", "get_hits", "get_misses", "bytes_read", "bytes_written"}
		if old != nil && len(old) > 2 {
			for _, k := range keys {
				if v, ok := st[k]; ok {
					if ov, ok := old[k]; ok {
						st["curr_"+k] = v.(uint64) - ov.(uint64)
					} else {
						log.Print("no in old", k)
					}
				} else {
					log.Print("no", k)
				}
			}
		} else {
			for _, k := range keys {
				st["curr_"+k] = uint64(0)
			}
			st["curr_uptime"] = uint64(1)
		}
		st["curr_hit"] = st["curr_get_hits"].(uint64) * 100 / (st["curr_cmd_get"].(uint64) + 1)
		st["curr_getset"] = float32(st["curr_cmd_get"].(uint64)) / float32(st["curr_cmd_set"].(uint64)+1.0)
		keys = []string{"cmd_get", "cmd_set", "cmd_delete", "bytes_read", "bytes_written"}

		server_stats[i] = st
	}
}

const STATIC_DIR = "/home/jwzh/goproject/src/github.com/JWZH/caskdb/master/static/"

func init() {
	funcs := make(template.FuncMap)
	funcs["in"] = in
	funcs["sum"] = sum
	funcs["size"] = sizer
	funcs["num"] = number
	funcs["time"] = timer

	tmpls = new(template.Template)
	tmpls = tmpls.Funcs(funcs)
	tmpls = template.Must(tmpls.ParseFiles(STATIC_DIR+"index.html", STATIC_DIR+"header.html",
		STATIC_DIR+"matrix.html", STATIC_DIR+"server.html"))
}

func Status(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")

	sections := req.FormValue("sections")
	if len(sections) == 0 {
		sections = "IN|SS"
	}
	all_sections := [][]string{}
	last := "U"
	for _, s := range SECTIONS {
		now := "U"
		if strings.Contains(sections, s[0]) {
			now = "S"
		}
		all_sections = append(all_sections, []string{last + now, s[0], s[1]})
		last = now
	}

	data := make(map[string]interface{})
	data["sections"] = sections
	data["all_sections"] = all_sections
	data["server_stats"] = server_stats
	data["proxy_stats"] = proxy_stats

	//st := schd.Stats()
	stats := make([]map[string]interface{}, len(server_stats))
	for i, _ := range stats {
		d := make(map[string]interface{})
		name := server_stats[i]["name"].(string)
		d["name"] = name
		d["stat"] = 0
		stats[i] = d
	}
	data["stats"] = stats
	err := tmpls.ExecuteTemplate(w, "index.html", data)
	if err != nil {
		println("render", err.Error())
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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
			if len(oldServers) < len(newServers) {
				client.UpdateServers(newServers)
				oldServers = newServers
				server_stats = make([]map[string]interface{}, len(newServers))
			}
		}
	}
	update_stats(oldServers, nil, server_stats, false)
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

	if port, e := c.Int("monitor", "port"); e != nil {
		log.Print("no port in conf", e.Error())
	} else {
		server_stats = make([]map[string]interface{}, len(servers))
		go update_stats(servers, nil, server_stats, true)

		//		proxys, e := c.String("monitor", "proxy")
		//		if e != nil {
		//			proxys = fmt.Sprintf("localhost:%d", port)
		//		}
		//		proxies := strings.Split(proxys, ",")
		//		proxy_stats := make([]map[string]interface{}, 1)
		//		go update_stats(proxies, nil, proxy_stats, false)

		http.Handle("/", http.HandlerFunc(makeGzipHandler(Status)))
		http.Handle("/static/", http.FileServer(http.Dir("./")))
		go func() {
			listen, e := c.String("monitor", "listen")
			if e != nil {
				listen = "0.0.0.0"
			}
			addr := fmt.Sprintf("%s:%d", listen, port)
			lt, e := net.Listen("tcp", addr)
			if e != nil {
				log.Println("monitor listen failed on ", addr, e)
			}
			log.Println("monitor listen on ", addr)
			http.Serve(lt, nil)
		}()
	}
	AllocLimit = *allocLimit
	if *debug {
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
	client = NewClient(schd)

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
