/*
   Implementation of Bitcask key-value store
*/

//TODO
// Merge is not finished !!

package bitcask

import (
	"errors"
	"fmt"
	. "github.com/JWZH/caskdb/bitcask/dict"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

const (
	LOGFILE         string      = "/var/test.log"
	defaultFilePerm os.FileMode = 0666
	defaultDirPerm  os.FileMode = 0766
	DATA_FILE       string      = "%09d.data"
)

type Stats struct {
	sum       int64
	dead      int64
	isMerging bool
}

type Options struct {
	MaxFileSize  int32
	MergeWindow  [2]int // startTime-EndTime
	MergeTrigger float32
	Path         string
}

type Bitcask struct {
	Stats
	Options
	sync.Mutex
	curr   *Bucket // active file
	keydir *Keydir
}

var ErrKeyNotFound = errors.New("Key not found")
var Lg *log.Logger

// Set log file
func init() {
	os.Remove(LOGFILE)
	logfile, _ := os.OpenFile(LOGFILE, os.O_RDWR|os.O_CREATE, defaultFilePerm)
	Lg = log.New(logfile, "\n", log.Ldate|log.Ltime|log.Llongfile)
}

// NewBitcask creates directory(path),
// and scans directory to build keydir
func NewBitcask(o Options) (*Bitcask, error) {

	err := os.MkdirAll(o.Path, defaultDirPerm)
	if err != nil {
		return nil, fmt.Errorf("Make dir %s %s", o.Path, err.Error())
	}

	b := new(Bitcask)
	b.keydir = NewKeydir()
	// set options
	b.Options = o
	// set stats
	b.isMerging = false
	b.sum, b.dead = 0, 0

	err = b.scan()
	go b.merge()

	return b, err
}

// Sync active file
// Release keydir
// Close active file
func (b *Bitcask) Close() error {
	if err := b.Sync(); err != nil {
		return err
	}
	b.keydir.Destroy()
	return b.curr.Close()
}
func (b *Bitcask) Set(key string, value []byte) error {
	b.Lock()
	defer b.Unlock()
	e := b.set2(key, value, time.Now().Unix())
	return e
}

func (b *Bitcask) Get(key string) ([]byte, error) {
	item, ok := b.keydir.Get(key)
	if !ok {
		return nil, ErrKeyNotFound
	}
	value, err := b.getValue(item)
	return value, err
}

// Set [0]
func (b *Bitcask) Del(key string) error {
	b.Lock()
	defer b.Unlock()

	value := []byte{0}
	e := b.set2(key, value, time.Now().Unix())
	b.keydir.Remove(key)
	if e != nil {
		b.dead++
	}
	return e
}

func (b *Bitcask) Sync() error {
	return b.curr.Sync()
}

func (b *Bitcask) Has(key string) bool {
	_, ok := b.keydir.Get(key)
	return ok
}

/*********** Private Methods ****************/

func (b *Bitcask) fillKeydir(fn string) error {
	f, err := os.Open(path.Join(b.Path, fn))
	if err != nil {
		return fmt.Errorf("FillKeydir : %s", err.Error())
	}
	defer f.Close()

	var fid int32
	fmt.Sscanf(fn, DATA_FILE, &fid)
	bucket := NewBucket(f, fid)
	var (
		toreturn error
		offset   int32 = 0
	)
	for {
		r, err := bucket.Read()
		if err != nil {
			if err != io.EOF {
				toreturn = err
			}
			break
		}
		offset += RECORD_HEADER_SIZE + r.ksz + r.vsz
		key := string(r.key)
		if b.isMerging {
			b.Lock()
			if it, ok := b.keydir.Get(key); ok {
				if r.tstamp == it.Tstamp {
					b.sum--
					err = b.set2(key, r.value, r.tstamp)
					if err != nil {
						return fmt.Errorf("Failed to set", err.Error())
					}
				}
			} else {
				b.dead--
			}
			b.Unlock()
		} else {
			// valid item
			if r.vsz != 1 || r.value[0] != 0 {
				b.sum++
				b.keydir.Add(key, fid, r.vsz, offset-r.vsz, r.tstamp)
				if b.Has(key) {
					b.dead++
				}
			} else { // invalid item(delete)
				b.dead++
			}
		}
	}
	return toreturn
}

func (b *Bitcask) set2(key string, value []byte, tstamp int64) error {
	if len(key) == 0 {
		return fmt.Errorf("Key can not be None")
	}
	if RECORD_HEADER_SIZE+int32(len(key)+len(value))+b.curr.offset > b.MaxFileSize {
		if err := b.curr.Close(); err != nil {
			return fmt.Errorf("Close %s failed: %s", b.curr.io.Name(), err.Error())
		}
		nbid := b.curr.id + 1
		npath := path.Join(b.Path, fmt.Sprintf(DATA_FILE, nbid))
		nfp, err := os.OpenFile(npath, os.O_CREATE|os.O_APPEND|os.O_RDWR, defaultDirPerm)
		if err != nil {
			return fmt.Errorf("Create %s failed :%s", nfp.Name(), err.Error())
		}
		b.curr = NewBucket(nfp, nbid)
	}
	vpos, err := b.curr.Write(key, value, tstamp)
	if err != nil {
		return err
	}
	b.keydir.Add(key, b.curr.id, int32(len(value)), vpos, tstamp)
	b.sum++
	if b.Has(key) {
		b.dead++
	}
	return nil

}

// scan reads key info from datafiles to build keydir
// Choose active file(RW)

func (b *Bitcask) scan() error {
	fns, err := getFileNames(b.Path)
	if err != nil {
		return err
	}
	for _, f := range fns {
		err := b.fillKeydir(f)
		if err != nil {
			return fmt.Errorf("scan fillkeydir : %s", err.Error())
		}
	}
	// choose active file
	var activeFilePath string
	var fid int32
	if len(fns) == 0 {
		activeFilePath = path.Join(b.Path, fmt.Sprintf(DATA_FILE, 0))
		fid = 0
	} else {
		activeFilePath = path.Join(b.Path, fns[len(fns)-1])
		fid = int32(len(fns)) - 1
	}

	Lg.Println("Open activefile " + activeFilePath)
	var activefile *os.File
	activefile, err = os.OpenFile(activeFilePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0766)
	b.curr = NewBucket(activefile, fid)

	return err
}

func (b *Bitcask) merge() {
	for {
		if float32(b.dead)/float32(b.sum) > b.MergeTrigger {
			h := time.Now().Hour()
			if h <= b.MergeWindow[1] &&
				h >= b.MergeWindow[0] {
				b.doMerge()
			}
		}
		time.Sleep(10 * time.Minute)
	}
}

func (b *Bitcask) doMergeLater() {
	time.Sleep(time.Minute)
	b.doMerge()
}

// TODO
// lack the ability to handle error
func (b *Bitcask) doMerge() {
	b.Lock()
	fns, err := getFileNames(b.Path)
	b.Unlock()
	if err != nil {
		b.doMergeLater()
		return
	}
	// active file cann't be merged !!
	fns = fns[:len(fns)-1]

	b.isMerging = true
	for _, fn := range fns {
		if err := b.fillKeydir(fn); err != nil {
			b.isMerging = false
			if err != nil {
				b.doMergeLater()
				return
			}
		}
		// try to be noteless
		time.Sleep(10 * time.Second)
	}
	// Guarantee that old data files are not handled
	time.Sleep(10 * time.Minute)

	for _, fn := range fns {
		os.Remove(path.Join(b.Path, fn))
	}
	return
}

func (b *Bitcask) getValue(item *Item) ([]byte, error) {
	fp, err := os.Open(path.Join(b.Path, fmt.Sprintf(DATA_FILE, item.Fid)))
	if err != nil {
		return nil, fmt.Errorf("getValue %s", err.Error())
	}
	defer fp.Close()

	value := make([]byte, item.Vsz)
	rsz, err := fp.ReadAt(value, int64(item.Vpos))
	if int32(rsz) != item.Vsz {
		return nil, fmt.Errorf("Expected %d bytes got %d", item.Vsz, rsz)
	}
	return value, nil
}

func getFileNames(dirPath string) ([]string, error) {
	var (
		dir *os.File
		err error
	)
	if dir, err = os.Open(dirPath); err != nil {
		return nil, err
	}
	defer dir.Close()

	fns, _ := dir.Readdirnames(-1)
	sort.Strings(fns)
	return fns, nil
}
