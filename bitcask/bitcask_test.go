package bitcask

import (
	"math/rand"
	"os"
	"testing"
)

const (
	testDirPath     = "/home/jwzh/testData"
	M           int = 1024 * 1024 * 1024 // 1G
	K           int = 1024
)

var O Options = Options{
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
	TestKeyValue{"key2", []byte("value3"), 4, 6},
}

func genValue(size int) []byte {
	v := make([]byte, size)
	for i := 0; i < size; i++ {
		v[i] = uint8((rand.Int() % 26) + 97) // a-z
	}
	return v
}

func TestBCOpen(t *testing.T) {

	b, err := NewBitcask(O)
	defer os.RemoveAll(b.Path)
	if err != nil {
		t.Errorf("Error \"%q\" while opening directory \"%q\"", err.Error(), "testkv")
	}
	err = b.Close()
	if err != nil {
		t.Errorf("Error \"%q\" while closing casket", err.Error())
	}
}

func TestBC(t *testing.T) {
	b, _ := NewBitcask(O)
	//	defer os.RemoveAll(b.path)
	for _, kv := range Testdata {
		err := b.Set(kv.key, kv.value)
		if err != nil {
			t.Fatalf("Error %s while Seting %s", err.Error(), kv.key)
		}
	}
	b.Sync()
	for _, kv := range Testdata {
		v, err := b.Get(kv.key)
		if err != nil {
			t.Fatalf("Error %s while Geting %s", err.Error(), kv.key)
		}
		if string(v) != string(kv.value) {
			t.Fatalf("Exptected %s, got %s", string(kv.value), string(v))
		}
	}
}

func BenchmarkSet1K(t *testing.B) {
	benchSet(t, K)
}

func BenchmarkSet1M(t *testing.B) {
	benchSet(t, K)
}

func BenchmarkGet1K(t *testing.B) {
	benchGet(t, K)
}

func BenchmarkGet1M(t *testing.B) {
	benchGet(t, M)
}

func benchSet(t *testing.B, size int) {
	//os.RemoveAll(testDirPath)
	b, _ := NewBitcask(O)
	value := genValue(size)
	t.SetBytes(int64(size))

	t.StartTimer()
	for i := 0; i < t.N; i++ {
		b.Set(string(i), value)
	}
	t.StopTimer()
	b.Close()
}

func benchGet(t *testing.B, size int) {
	b, _ := NewBitcask(O)
	t.SetBytes(int64(size))
	t.StartTimer()
	for i := 0; i < t.N; i++ {
		b.Get(string(i))
	}
	t.StopTimer()
	b.Close()
}
