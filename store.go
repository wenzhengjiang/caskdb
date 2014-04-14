package caskdb

import (
	. "github.com/JWZH/caskdb/bitcask"
)

type Storage interface {
	Get(key string) (*Item, error)
	Set(key string, item *Item, noreply bool) (bool, error)
	Delete(key string) error
	//	Len() int
}

type BitcaskStore struct {
	bc *Bitcask
}

func NewBitcaskStore(c Config) *BitcaskStore {
	b := new(BitcaskStore)
	b.bc = new(Bitcask)
	var err error
	b.bc, err = NewBitcask(c.Options)
	if err != nil {
		panic("Can not open db:" + c.Path + err.Error())
	}
	return b
}

func (self *BitcaskStore) Close() error {
	if err := self.bc.Close(); err != nil {
		return err
	}
	return nil
}

func (self *BitcaskStore) Get(key string) (*Item, error) {
	v, err := self.bc.Get(key)
	if err != nil {
		return nil, err
	}
	return &Item{Body: v}, nil
}

func (self *BitcaskStore) Set(key string, item *Item) error {
	return self.bc.Set(key, item.Body)
}

func (self *BitcaskStore) Delete(key string) error {
	return self.bc.Del(key)
}
