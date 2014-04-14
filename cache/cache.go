package cache

import (
	"sync"
)

type Cache struct {
	sync.RWMutex
	maxSize uint32 // 4G
	size    uint32
	l       *List
	ht      map[string][]byte
}

func NewCache(maxSize uint32) *Cache {
	return &Cache{
		maxSize: maxSize,
		size:    0,
		l:       NewList(),
		ht:      make(map[string][]byte)}
}

// O(n) // asynchronous
// if size > maxSize, then remove recently unused item until size <= maxSize

func (c *Cache) Set(key string, value []byte) {
	go func() {
		c.Lock()
		defer c.Unlock()

		if _, ok := c.ht[key]; ok {
			for e := c.l.Front(); e != nil; e = e.Next() {
				if e.Value == key {
					c.l.Remove(e)
					return
				}
			}
			c.l.PushFront(key)
		} else {
			// assume numbers of items must be >= 2
			if uint32(len(value))+c.size > c.maxSize {
				for e := c.l.Back().Prev(); e != c.l.Front(); e = e.Prev() {
					c.size -= uint32(len(e.Next().Value))
					delete(c.ht, e.Next().Value)
					c.l.Remove(e.Next())

					if uint32(len(value))+c.size < c.maxSize {
						break
					}
				}
			} else {
				c.ht[key] = value
				c.l.PushFront(key)
				c.size += uint32(len(value))
			}
		}
		return
	}()
	return
}

func (c *Cache) Get(key string) []byte {
	c.RLock()
	defer c.RUnlock()
	if v, ok := c.ht[key]; ok {
		return v
	}
	return nil
}

func (c *Cache) Delete(key string) {
	c.Lock()
	defer c.Unlock()
	delete(c.ht, key)
}

func (c *Cache) Has(key string) bool {
	c.RLock()
	defer c.RUnlock()

	if _, ok := c.ht[key]; ok {
		return true
	}
	return false
}
