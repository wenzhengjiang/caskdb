package protocol

import (
	"testing"
)

func TestHost(t *testing.T) {
	store := NewMapStore()
	server := NewServer(store)
	server.Listen("localhost:7901")
	go server.Serve()
	host := NewHost("localhost:7901")
	_, e := host.Set("key", &Item{Body: []byte{1}}, false)
	if e != nil {
		t.Errorf("Set %s\n", e.Error())
	}
	_, e = host.Get("key")
	if e != nil {
		t.Errorf("Get %s\n", e.Error())
	}
}
