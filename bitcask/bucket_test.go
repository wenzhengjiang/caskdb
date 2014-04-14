package bitcask

import (
	"os"
	"testing"
)

const testFilePath = "/tmp/1"

func TestBasic(t *testing.T) {
	defer os.Remove(testFilePath)
	f, _ := os.OpenFile(testFilePath, os.O_CREATE|os.O_RDWR, 0666)
	b := NewBucket(f, 1)
	for _, kv := range Testdata {
		_, err := b.Write(kv.key, kv.value, 0)
		if err != nil {
			t.Fatalf("Error %s while writing %s", err.Error(), kv.key)
		}
	}
	b.Sync()
	b.io.Seek(0, 0)
	for _, kv := range Testdata {
		r, err := b.Read()
		if err != nil {
			t.Fatalf("Error %s while reading %s", err.Error(), kv.key)
		}
		if string(r.key) != kv.key {
			t.Fatalf("Exptected %s, got %s", kv.key, string(r.key))
		}
		if string(r.value) != string(kv.value) {
			t.Fatalf("Exptected %s, got %s", string(kv.value), string(r.value))
		}
	}
}

/*
func BenchmarkWrite_1K_NoCompress(b *testing.B) {
	benchWrite(b, K, false)
}

func BenchmarkWrite_10K_NoCompress(b *testing.B) {
	benchWrite(b, 10*K, false)
}

func BenchmarkWrite_100K_NoCompress(b *testing.B) {
	benchWrite(b, 100*K, false)
}

func BenchmarkWrite_1M_NoCompress(b *testing.B) {
	benchWrite(b, M, false)
}

func BenchmarkWrite_10M_NoCompress(b *testing.B) {
	benchWrite(b, 10*M, false)
}

func BenchmarkWrite_50M_NoCompress(b *testing.B) {
	benchWrite(b, 50*M, false)
}

func BenchmarkWrite_100M_NoCompress(b *testing.B) {
	benchWrite(b, 100*M, false)
}

func BenchmarkWrite_1K_Compress(b *testing.B) {
	benchWrite(b, 1*K, true)
}

func BenchmarkWrite_10K_Compress(b *testing.B) {
	benchWrite(b, 10*K, true)
}

func BenchmarkWrite_100K_Compress(b *testing.B) {
	benchWrite(b, 100*K, true)
}

func BenchmarkWrite_1M_Compress(b *testing.B) {
	benchWrite(b, M, true)
}

func BenchmarkWrite_10M_Compress(b *testing.B) {
	benchWrite(b, 10*M, true)
}

func BenchmarkWrite_50M_Compress(b *testing.B) {
	benchWrite(b, 50*M, true)
}

func BenchmarkWrite_100M_Compress(b *testing.B) {
	benchWrite(b, 100*M, true)
}
*/
func benchWrite(t *testing.B, size int) {
	os.Remove(testFilePath)
	f, _ := os.OpenFile(testFilePath, os.O_CREATE|os.O_RDWR, 0666)
	b := NewBucket(f, 1)
	value := genValue(size)

	t.SetBytes(int64(size))
	t.StartTimer()
	for i := 0; i < t.N; i++ {
		b.Write(string(i), value, 1)
	}
	t.StopTimer()
	b.Close()
}
