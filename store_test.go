package caskdb

import (
	"github.com/JWZH/caskdb/bitcask"
	"os"
	"testing"
)

const testDirPath = "/home/jwzh/testdata"

var O bitcask.Options = bitcask.Options{
	MaxFileSize:  1024 * 1024,
	MergeWindow:  [2]int{0, 23},
	MergeTrigger: 0.6,
	Path:         testDirPath,
}

type TestKeyValue struct {
	key      string
	value    []byte
	ksz, vsz int32
}

var Testdata = []TestKeyValue{
	TestKeyValue{"key1", []byte("value1"), 4, 6},
	TestKeyValue{"key2", []byte("value2"), 4, 6},
	TestKeyValue{"key3", []byte("value3"), 4, 6},
	TestKeyValue{"key4", []byte("value4"), 4, 6},
}

func TestOpen(t *testing.T) {

	bc := NewBitcaskStore(Config{O})
	err := bc.Close()
	if err != nil {
		t.Fatalf("Error %s Close", err.Error())
	}
}

func TestW(t *testing.T) {
	bc := NewBitcaskStore(Config{O})
	for _, kv := range Testdata {
		err := bc.Set(kv.key, &Item{Body: kv.value})
		if err != nil {
			t.Fatalf("Error %s while Seting %s", err.Error(), kv.key)
		}
	}
	bc.Close()
}

func TestR(t *testing.T) {
	bc := NewBitcaskStore(Config{O})
	defer os.RemoveAll(testDirPath)
	for _, kv := range Testdata {
		i, err := bc.Get(kv.key)
		if err != nil {
			t.Fatalf("Error %s while Geting %s", err.Error(), kv.key)
		}
		if string(i.Body) != string(kv.value) {
			t.Fatalf("Exptected %s, got %s", string(kv.value), string(i.Body))
		}
	}
}
