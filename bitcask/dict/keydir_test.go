package dict

import (
	"fmt"
	"testing"
	"time"
)

const N int = 1024 * 1024 * 10

/*
Benchmark 10,000,000

/*
Add  +1.830080e+001
Add 572967.145810 ops/sec
Get  +1.747524e+001
Get 600035.149886 ops/sec
*/
func TestRW(t *testing.T) {
	kv := NewKeydir()
	t0 := time.Now()
	for i := 0; i < N; i++ {
		kv.Add(fmt.Sprintf("%d", i), 1, 1, 1, 1)
	}
	t1 := time.Now()
	println("Add ", t1.Sub(t0).Seconds())
	print(fmt.Sprintf("Add %f ops/sec\n", float64(N)/t1.Sub(t0).Seconds()))

	t0 = time.Now()
	for i := 0; i < N; i++ {
		item, e := kv.Get(fmt.Sprintf("%d", i))
		if !e || item.Tstamp != 1 {
			t.Error("Failed to get correct item")
		}
	}
	t1 = time.Now()
	println("Get ", t1.Sub(t0).Seconds())
	print(fmt.Sprintf("Get %f ops/sec\n", float64(N)/t1.Sub(t0).Seconds()))

	kv.Destroy()
}
