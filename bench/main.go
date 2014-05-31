package main

import (
	. "caskdb/client"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"strconv"
	"time"
)

var t *string = flag.String("t", "W", "")
var N *int = flag.Int("n", 1000, "")
var vsz *string = flag.String("sz", "1K", "")
var addr *string = flag.String("addr", "localhost:7905", "")
var thread *int = flag.Int("thread", 4, "")
var conn *int = flag.Int("conns", 1, "")
var dua *int = flag.Int("dua", 0, "")

func genValue(size int) []byte {
	v := make([]byte, size)
	for i := 0; i < size; i++ {
		v[i] = uint8((rand.Int() % 26) + 97) // a-z
	}
	return v
}

//a_b
func benchSet(s *Client, N, vsz int) time.Duration {
	value := genValue(vsz)
	ok := make(chan bool, *conn)
	t0 := time.Now()
	for i := 0; i < *conn; i++ {
		go func(i int) {
			for j := 0; j < N / *conn; j++ {
				if *dua > 0 {
					time.Sleep(time.Duration(*dua) * time.Millisecond)
				}
				key := fmt.Sprintf("%d_%d", i, j)
				_, err := s.Set(key, value)
				if err != nil {
					log.Fatalf("Error %s while Seting %s", err.Error(), key)
				}
			}
			ok <- true
		}(i)
	}
	for i := 0; i < *conn; i++ {
		<-ok
	}
	t1 := time.Now()
	s.FlushAll()
	return t1.Sub(t0)
}

func benchSetSync(s *Client, N, vsz int) time.Duration {
	// value := genValue(vsz)
	t0 := time.Now()
	//	for j := 0; j < N; j++ {
	//		key := fmt.Sprintf("%d_%d", j%16, j)
	//		err := s.Set(key, value)
	//		s.Sync()
	//		if err != nil {
	//			log.Fatalf("Error %s while Seting %s", err.Error(), key)
	//		}
	//	}
	t1 := time.Now()
	return t1.Sub(t0)
}
func benchGet(s *Client, N, vsz int) time.Duration {
	benchSet(s, N, vsz)
	ok := make(chan bool, *conn)
	t0 := time.Now()
	for i := 0; i < *conn; i++ {
		go func(i int) {
			for j := 0; j < N / *conn; j++ {
				key := fmt.Sprintf("%d_%d", i, j)
				_, err := s.Get(key)
				if err != nil {
					log.Fatalf("Error %s while Geting %s", err.Error(), key)
				}
			}
			ok <- true
		}(i)
	}
	for i := 0; i < *conn; i++ {
		<-ok
	}
	t1 := time.Now()
	return t1.Sub(t0)
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*thread)
	client := NewClient(*addr)
	f := func(s string) int {
		ret, _ := strconv.Atoi(s[:len(s)-1])
		switch s[len(s)-1] {
		case 'B':
			return ret
		case 'K':
			return ret * 1024
		case 'M':
			return ret * 1024 * 1024
		}
		return ret
	}

	fmt.Printf("Benchmark %s vsz = %s, ", *t, *vsz)
	var du time.Duration
	switch *t {
	case "W":
		du = benchSet(client, *N, f(*vsz))
	case "R":
		du = benchGet(client, *N, f(*vsz))
	case "SW":
		du = benchSetSync(client, *N, f(*vsz))
	}

	fmt.Printf("%f ops/sec\n", float64(*N)/du.Seconds())

	client.FlushAll()
	return
}
