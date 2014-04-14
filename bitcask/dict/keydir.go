/*
Impletation of keydir
   | --- |      | -------------------------------------------------------------------------- |
   | key | -->  | file id (int32) | value size (int32) | value pos (int32) | Tstamp (int64) |
   | --- |      | -------------------------------------------------------------------------- |

*/

package dict

//#include "dict.h"
import "C"
import "sync"
import "fmt"
import "unsafe"

const itemSize int = 20

type Item struct {
	Fid    int32
	Vsz    int32
	Vpos   int32
	Tstamp int64
}

// Keydir is a index data structure for bitcask
// It wrap for hashmap(builtin go)
// It is safe to call add, remove, get concurrently.
type Keydir struct {
	sync.RWMutex
	dict *C.Dict
}

func NewKeydir() *Keydir {
	return &Keydir{
		dict: C.dict_new(),
	}
}

func (k *Keydir) Add(key string, Fid, Vsz, Vpos int32, Tstamp int64) error {
	k.Lock()
	defer k.Unlock()

	c_key := C.CString(key)
	defer C.free(unsafe.Pointer(c_key))
	sliceKey := C.slice_new(c_key, C.int((len(key))))

	var item C.Item
	item.fid = C.int(Fid)
	item.vsz = C.int(Vsz)
	item.vpos = C.int(Vpos)
	item.tstamp = C.longlong(Tstamp)

	ok := C.dict_add(k.dict, sliceKey, item)
	if ok != C.DICT_OK {
		return fmt.Errorf("failed to add Key %s", key)
	}
	return nil
}

func (k *Keydir) Get(key string) (*Item, bool) {
	k.RLock()
	defer k.RUnlock()

	c_key := C.CString(key)
	defer C.free(unsafe.Pointer(c_key))
	sliceKey := C.slice_new(c_key, C.int((len(key))))

	c_item := C.dict_get(k.dict, sliceKey)
	if int(c_item.fid) == -1 {
		return nil, false
	}

	return &Item{
		Fid:    int32(c_item.fid),
		Vsz:    int32(c_item.vsz),
		Vpos:   int32(c_item.vpos),
		Tstamp: int64(c_item.tstamp),
	}, true
}

func (k *Keydir) Remove(key string) {
	k.Lock()
	defer k.Unlock()
	t_key := C.CString(key)
	defer C.free(unsafe.Pointer(t_key))
	C.dict_delete(k.dict, C.slice_new(t_key, C.int(len(key))))
}

func (k *Keydir) Destroy() {
	k.Lock()
	defer k.Unlock()
	C.dict_destroy(k.dict)
}
