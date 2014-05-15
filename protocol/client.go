/*
 * memcache client
 */

package protocol

import (
	"fmt"
)

type MODE int

const (
	SET MODE = iota
	GET
	DELETE
)

// Client of memcached
type Client struct {
	sch *Scheduler
}

func NewClient(sch *Scheduler) (c *Client) {
	c = new(Client)
	c.sch = sch
	return c
}

func (c *Client) Get(key string) (r *Item, err error) {
	hosts := c.sch.GetHostsByKey(key)
	for _, h := range hosts {
		r, err := h.Get(key)
		if err == nil {
			return r, nil
		} else {
			err = fmt.Errorf("%s : %s", h.Addr, err.Error())
		}
	}
	return
}

func (c *Client) Set(key string, item *Item, noreply bool) (bool, error) {
	hosts := make([]*Host, 1)
	if c.sch.IsMegrating {
		hosts = c.sch.GetHostsByKey2(key)
	} else {
		hosts = c.sch.GetHostsByKey(key)
	}
	for i, h := range hosts {
		if i == 0 {
			ok, e := h.Set(key, item, noreply)
			if !ok || e != nil {
				return ok, fmt.Errorf("%s : %s", h.Addr, e.Error())
			}
		} else {
			go h.Set(key, item, noreply)
		}
	}
	return true, nil
}

func (c *Client) Delete(key string) (r bool, err error) {
	if c.sch.IsMegrating {
		hosts := c.sch.GetHostsByKey2(key)
		for _, h := range hosts {
			ok, e := h.Delete(key)
			if !ok || e != nil {
				return ok, e
			}
		}
	}
	hosts := c.sch.GetHostsByKey(key)
	for _, h := range hosts {
		ok, e := h.Delete(key)
		if !ok || e != nil {
			return ok, e
		}
	}
	return true, nil
}

func (c *Client) FlushAll() {
	for _, h := range c.sch.hosts {
		h.FlushAll()
	}
}
func (c *Client) UpdateServers(addrs []string) {
	c.sch.Update(addrs)
}

func (c *Client) Len() int64 {
	return 0
}
