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
	hosts := make([]*Host, 2)
	if c.sch.IsMegrating {
		hosts = c.sch.GetHostsByKey2(key)
	} else {
		hosts = c.sch.GetHostsByKey(key)
	}

	if len(hosts) == 2 {
		key2 := key + "@#$" + hosts[1].Addr
		ok, e := hosts[0].Set(key2, item, noreply)
		// treat hosts[1] to the main copy node
		if !ok {
			key2 = key + "@#$" + hosts[0].Addr
			ok, e = hosts[1].Set(key2, item, noreply)
		}
		if !ok || e != nil {
			return ok, fmt.Errorf("%s : %s", hosts[1].Addr, e.Error())
		}
	} else {
		ok, e := hosts[0].Set(key, item, noreply)
		if !ok || e != nil {
			return ok, fmt.Errorf("%s : %s", hosts[0].Addr, e.Error())
		}
	}
	return true, nil
}

func (c *Client) Delete(key string) (r bool, err error) {
	hosts := make([]*Host, 2)
	if c.sch.IsMegrating {
		hosts = c.sch.GetHostsByKey2(key)
	} else {
		hosts = c.sch.GetHostsByKey(key)
	}

	if len(hosts) == 2 {
		key += "@@" + hosts[1].Addr
	}
	ok, e := hosts[0].Delete(key)
	if !ok || e != nil {
		return ok, fmt.Errorf("%s : %s", hosts[0].Addr, e.Error())
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
