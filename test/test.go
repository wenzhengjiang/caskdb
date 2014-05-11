package main

import (
	"fmt"
	. "github.com/JWZH/caskdb/memcache"
	"log"
	"math/rand"
)

func genValue(size int) []byte {
	v := make([]byte, size)
	for i := 0; i < size; i++ {
		v[i] = uint8((rand.Int() % 26) + 97) // a-z
	}
	return v
}

const N = 1000000
const size = 1024

func main() {
	client := NewHost("localhost:7905")
	value := genValue(size)
	for i := 0; i < N; i++ {
		_, e := client.Set("key"+fmt.Sprint(i), &Item{Body: value}, false)
		if e != nil {
			log.Fatal(e)
		} else {
			log.Println(i)
		}
	}
	for i := 0; i < N; i++ {
		_, e := client.Get("key")
		if e != nil {
			log.Fatal(e)
		} else {
			log.Println(i)
		}
	}
}
