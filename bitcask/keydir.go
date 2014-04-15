/*
   Impletation of keydir
   | --- |      | --------------------------------------------------------------------------|
   | key | -->  | file id (int32) | value size (int32) | value pos (int32) | Tstamp (int64) |
   | --- |      | --------------------------------------------------------------------------|
*/

package bitcask

import (
	"sync"
)

type Item struct {
	Fid    int32
	Vsz    int32
	Vpos   int32
	Tstamp int64
}

// Keydir is a index data structure for bitcask
// It wrap for hashmap(builtin go)
// It is safe to call add, remove, get concurrently.
type Keydir struct {
	sync.RWMutex
	kv map[string]Item
}

func NewKeydir() *Keydir {
	return &Keydir{
		kv: make(map[string]Item),
	}
}

func (k *Keydir) Add(key string, Fid, Vsz, Vpos int32, Tstamp int64) error {
	k.Lock()
	defer k.Unlock()

	k.kv[key] = Item{Fid, Vsz, Vpos, Tstamp}

	return nil
}

func (k *Keydir) Get(key string) (*Item, bool) {
	k.RLock()
	defer k.RUnlock()

	v, b := k.kv[key]
	return &v, b
}

func (k *Keydir) Remove(key string) {
	k.Lock()
	defer k.Unlock()

	delete(k.kv, key)
}
func (k *Keydir) Destroy() {

}

//const N int = 10000000
//
//func main() {
//	kv := NewKeydir()
//	t0 := time.Now()
//	for i := 0; i < N; i++ {
//		kv.add(string(i), 1, 1, 1, 1)
//	}
//	t1 := time.Now()
//	fmt.Printf("%f ops/sec\n", float64(N)/t1.Sub(t0).Seconds())
//}

//950575.158332 ops/sec
