package main

import (
	"flag"
	"fmt"
	. "github.com/JWZH/caskdb/bitcask"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"
)

const testDirPath = "/home/jwzh/tmpdata/testData"

var O Options = Options{1024 * 1024 * 1024, [2]int{0, 23}, 1.0, testDirPath}
var t *string = flag.String("t", "W", "")
var N *int = flag.Int("n", 1000, "")
var vsz *string = flag.String("sz", "1K", "")

func genValue(size int) []byte {
	v := make([]byte, size)
	for i := 0; i < size; i++ {
		v[i] = uint8((rand.Int() % 26) + 97) // a-z
	}
	return v
}

//a_b
func benchSet(s *Bitcask, N, vsz int) time.Duration {
	value := genValue(vsz)
	t0 := time.Now()
	for j := 0; j < N; j++ {
		key := fmt.Sprintf("%d_%d", j%16, j)
		err := s.Set(key, value)
		s.Sync()
		if err != nil {
			log.Fatalf("Error %s while Seting %s", err.Error(), key)
		}
	}
	t1 := time.Now()
	return t1.Sub(t0)
}
func benchSetSync(s *Bitcask, N, vsz int) time.Duration {
	value := genValue(vsz)
	t0 := time.Now()
	for j := 0; j < N; j++ {
		key := fmt.Sprintf("%d_%d", j%16, j)
		err := s.Set(key, value)
		s.Sync()
		if err != nil {
			log.Fatalf("Error %s while Seting %s", err.Error(), key)
		}
	}
	t1 := time.Now()
	return t1.Sub(t0)
}
func benchGet(s *Bitcask, N, vsz int) time.Duration {
	value := genValue(vsz)
	for j := 0; j < N; j++ {
		key := fmt.Sprintf("%d_%d", j%16, j)
		err := s.Set(key, value)
		if err != nil {
			log.Fatalf("Error %s while Seting %s", err.Error(), key)
		}
	}
	s.Sync()
	kv := make([]int, N)
	for i := 0; i < N; i++ {
		kv[i] = rand.Intn(N)
	}
	t0 := time.Now()
	for j := 0; j < N; j++ {
		key := fmt.Sprintf("%d_%d", kv[j]%16, kv[j])
		_, err := s.Get(key)
		if err != nil {
			log.Fatalf("Error %s while Geting %s", err.Error(), key)
		}
	}
	t1 := time.Now()
	return t1.Sub(t0)
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(4)
	os.RemoveAll(testDirPath)
	s, _ := NewBitcask(O)

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
		du = benchSet(s, *N, f(*vsz))
	case "R":
		du = benchGet(s, *N, f(*vsz))
	case "SW":
		du = benchSetSync(s, *N, f(*vsz))
	}
	fmt.Printf("%f ops/sec\n", float64(*N)/du.Seconds())

	t0 := time.Now()
	s.Close()
	t1 := time.Now()
	fmt.Printf("disk: %f ops/sec ", float64(*N)/(du.Seconds()+t1.Sub(t0).Seconds()))

	return
}
