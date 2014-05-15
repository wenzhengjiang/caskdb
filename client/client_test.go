package client

import (
	"testing"
)

func TestClient(t *testing.T) {
	store := NewMapStore()
	server := NewServer(store)
	server.Listen("localhost:7901")
	go server.Serve()
	host := NewClient("localhost:7901")
	_, e := host.Set("key", []byte{1})
	if e != nil {
		t.Errorf("Set %s\n", e.Error())
	}
	_, e = host.Get("key")
	if e != nil {
		t.Errorf("Get %s\n", e.Error())
	}
}
