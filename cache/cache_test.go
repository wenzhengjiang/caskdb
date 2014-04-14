package cache

import (
	"testing"
)

const M uint32 = 1024 * 1024

type TestKeyValue struct {
	key      string
	value    []byte
	ksz, vsz int32
}

var Testdata = []TestKeyValue{
	TestKeyValue{"key1", []byte("value1"), 4, 6},
	TestKeyValue{"key2", []byte("value2"), 4, 6},
}

func TestCache(t *testing.T) {
	c := NewCache(50 * M)
	for _, kv := range Testdata {
		c.Set(kv.key, kv.value)
	}
	for _, kv := range Testdata {
		value := c.Get(kv.key)
		if string(value) != string(kv.value) {
			t.Fatalf("Exptected %s, got %s", string(kv.value), string(value))
		}
	}
}
