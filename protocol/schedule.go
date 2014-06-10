package protocol

import (
	"log"
	"sort"
	"sync"
)

type uint64Slice []uint64

func (l uint64Slice) Len() int {
	return len(l)
}

func (l uint64Slice) Less(i, j int) bool {
	return l[i] < l[j]
}

func (l uint64Slice) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

type Scheduler struct {
	sync.RWMutex
	hosts, hosts2 []*Host
	index, index2 []uint64
	liveChan      string
	deadChan      string
	IsMegrating   bool
}

func NewScheduler(hosts []string) *Scheduler {
	var c Scheduler
	c.hosts = make([]*Host, len(hosts))
	c.index = make([]uint64, len(hosts))
	for i, h := range hosts {
		c.hosts[i] = NewHost(h)
		v := crc32hash([]byte(h))
		c.index[i] = (uint64(v) << 32) + uint64(i)
	}
	sort.Sort(uint64Slice(c.index))
	if !sort.IsSorted(uint64Slice(c.index)) {
		panic("sort failed")
	}
	c.IsMegrating = false
	return &c
}

func (c *Scheduler) getHostIndex(key string, index []uint64) []int {
	h := uint64(crc32hash([]byte(key))) << 32
	N := len(index)
	i := sort.Search(N, func(k int) bool { return index[k] >= h })
	if i == N {
		i = 0
	}
	id := int(index[i] & 0xffffffff)
	if N >= 2 {
		return []int{id, (id + 1) % N}
	}
	return []int{id}
}

func (c *Scheduler) GetHostsByKey(key string) []*Host {
	c.RLock()
	defer c.RUnlock()

	is := c.getHostIndex(key, c.index)
	r := make([]*Host, len(is))
	for i, k := range is {
		r[i] = c.hosts[k]
	}
	return r
}

func (c *Scheduler) GetHostsByKey2(key string) []*Host {
	c.RLock()
	defer c.RUnlock()

	is := c.getHostIndex(key, c.index2)
	r := make([]*Host, len(is))
	for i, k := range is {
		r[i] = c.hosts2[k]
	}
	return r
}

//TODO Need to be Better !

func (c *Scheduler) Update(addrs []string) {
	log.Println("Update")
	if len(addrs) == len(c.hosts) {
		return
	}
	c.hosts2 = make([]*Host, len(addrs))
	c.index2 = make([]uint64, len(addrs))
	for i, h := range addrs {
		c.hosts2[i] = NewHost(h)
		v := crc32hash([]byte(h))
		c.index2[i] = (uint64(v) << 32) + uint64(i)
	}
	sort.Sort(uint64Slice(c.index2))
	c.IsMegrating = true
	go func() {
		c.doMigrateJob()
		c.Lock()
		c.IsMegrating = false
		c.index = c.index2
		c.hosts = c.hosts2
		c.Unlock()
	}()
}

func (c *Scheduler) doMigrateJob() {
	//TODO need better solution!!
	log.Println("doMigrateJob")
	addr := c.hosts2[len(c.hosts2)-1].Addr
	N := len(c.hosts)
	v := uint64(crc32hash([]byte(addr)))<<32 + uint64(N)
	i := sort.Search(N, func(k int) bool { return c.index[k] >= v })
	hid := c.index[i] & 0xffffffff
	err := c.hosts[hid].Migrate(addr, uint32(c.index[(i-2+N)%N]>>32), uint32(v>>32))
	if err != nil {
		c.doMigrateJob()
	}
}

//// 1 2 3 U 5 6
//func (c *Scheduler) deleleNode(addr string) {
//	c.Lock()
//	defer c.Unlock()
//
//	var k int
//	for i, h := range c.hosts {
//		if h.Addr == addr {
//			k = i
//			break
//		}
//	}
//	c.hosts = append(c.hosts[0:k], c.hosts[k:]...)
//
//	v := uint64(fvn1a([]byte(addr)))<<32 + uint64(j)
//	i := sort.Search(len(c.index), func(int k) bool { return c.index[k] >= v })
//	N := len(c.index)
//	if N > 2 {
//		id := (i - 2 + N) % N
//		c.hosts[id].Migrate(i, c.index[(id-1+N)%N]>>32, c.index[id]>>32)
//	}
//	c.index = append(c.index[0:i], c.index[i:]...)
//}
//
