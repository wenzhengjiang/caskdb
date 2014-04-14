package caskdb

type Item struct {
	Cas  int
	Body []byte
}
