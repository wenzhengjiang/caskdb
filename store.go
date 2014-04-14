package caskdb

import (
	"./bitcask"
)

type Storage interface {
	Get(key string) (*Item, error)
	Set(key string, item *Item, noreply bool) (bool, error)
	Delete(key string) error
	//	Len() int
}

type BitcaskStore struct {
	bc *bitcask.Bitcask
}

func NewBitcaskStore(o Options) *BitcaskStore {
	b := new(BitcaskStore)
	b.bc = new(bitcask.Bitcask)
	var err error
	b.bc, err = bitcask.NewBitcask(bitcask.Options{
		MaxFileSize:  o.MaxFileSize,
		MergeWindow:  o.MergeWindow,
		MergeTrigger: o.MergeTrigger,
		Path:         o.Path,
	})
	if err != nil {
		panic("Can not open db:" + o.Path + err.Error())
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
