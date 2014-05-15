package protocol

import (
	"bufio"
	"caskdb/cmem"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"unsafe"
)

const VERSION = "0.1.0"

const (
	MaxKeyLength  = 200
	MaxBodyLength = 1024 * 1024 * 50
)

var AllocLimit = 1024 * 4

type Item struct {
	Body  []byte
	alloc *byte
}

func (it *Item) String() (s string) {
	return fmt.Sprintf("Item(Length:%d, Body:%v",
		len(it.Body), it.Body)
}

type Request struct {
	Cmd     string // get, set, delete, quit, etc.
	Key     string // keys
	Item    *Item
	NoReply bool
}

func (req *Request) String() (s string) {
	return fmt.Sprintf("Request(Cmd:%s, Key:%v, Item:%v, NoReply: %t)",
		req.Cmd, req.Key, &req.Item, req.NoReply)
}

func (req *Request) Clear() {
	req.NoReply = false
	if req.Item != nil && req.Item.alloc != nil {
		cmem.Free(req.Item.alloc, uintptr(cap(req.Item.Body)))
		req.Item.Body = nil
		req.Item.alloc = nil
		req.Item = nil
	}
}

func WriteFull(w io.Writer, buf []byte) error {
	n, e := w.Write(buf)
	for e != nil && n > 0 {
		buf = buf[n:]
		n, e = w.Write(buf)
	}
	return e
}

func (req *Request) Write(w io.Writer) (e error) {

	switch req.Cmd {

	case "get", "delete", "quit", "version", "stats", "flush_all":
		io.WriteString(w, req.Cmd)
		if req.Key != "" {
			io.WriteString(w, " "+req.Key)
		}
		if req.NoReply {
			io.WriteString(w, " noreply")
		}
		_, e = io.WriteString(w, "\r\n")

	case "set":
		noreplay := ""
		if req.NoReply {
			noreplay = " noreply"
		}
		item := req.Item
		fmt.Fprintf(w, "%s %s %d %s\r\n", req.Cmd, req.Key,
			len(item.Body), noreplay)
		if WriteFull(w, item.Body) != nil {
			return e
		}
		e = WriteFull(w, []byte("\r\n"))
	default:
		log.Printf("unkown request cmd:", req.Cmd)
		return errors.New("unknown cmd: " + req.Cmd)
	}

	return e
}

func (req *Request) Read(b *bufio.Reader) (e error) {
	var s string
	if s, e = b.ReadString('\n'); e != nil {
		return e
	}
	if !strings.HasSuffix(s, "\r\n") {
		return errors.New("not completed command")
	}
	parts := strings.Fields(s)
	if len(parts) < 1 {
		return errors.New("invalid cmd")
	}

	req.Cmd = parts[0]
	switch req.Cmd {

	case "get":
		if len(parts) != 2 {
			return errors.New("invalid cmd")
		}
		req.Key = parts[1]

	case "set":
		if len(parts) != 3 {
			return errors.New("invalid cmd")
		}
		req.Key = parts[1]
		req.Item = &Item{}
		item := req.Item
		length, e := strconv.Atoi(parts[2])
		if e != nil {
			return e
		}
		if length > MaxBodyLength {
			return errors.New("body too large")
		}
		// FIXME
		if length > AllocLimit {
			item.alloc = cmem.Alloc(uintptr(length))
			item.Body = (*[1 << 30]byte)(unsafe.Pointer(item.alloc))[:length]
			(*reflect.SliceHeader)(unsafe.Pointer(&item.Body)).Cap = length
			runtime.SetFinalizer(item, func(item *Item) {
				if item.alloc != nil {
					//log.Print("free by finalizer: ", cap(item.Body))
					cmem.Free(item.alloc, uintptr(cap(item.Body)))
					item.Body = nil
					item.alloc = nil
				}
			})
		} else {
			item.Body = make([]byte, length)
		}
		if _, e = io.ReadFull(b, item.Body); e != nil {
			return e
		}
		b.ReadByte() // \r
		b.ReadByte() // \n

	case "delete":
		if len(parts) != 2 {
			return errors.New("invalid cmd")
		}
		req.Key = parts[1]

	case "stats":
	case "quit", "version", "flush_all":
	default:
		log.Print("unknown command", req.Cmd)
		return errors.New("unknown command: " + req.Cmd)
	}

	return
}

type Response struct {
	status  string
	msg     string
	items   map[string]*Item
	noreply bool
}

func (resp *Response) String() (s string) {
	return fmt.Sprintf("Response(Status:%s, msg:%s, Items:%v)",
		resp.status, resp.msg, resp.items)
}

func (resp *Response) Read(b *bufio.Reader) error {
	resp.items = make(map[string]*Item, 1)
	for {
		s, e := b.ReadString('\n')
		if e != nil {
			log.Print("read response line failed", e)
			return e
		}
		parts := strings.Fields(s)
		if len(parts) < 1 {
			return errors.New("invalid response")
		}

		resp.status = parts[0]
		switch resp.status {

		case "VALUE":
			if len(parts) < 3 {
				return errors.New("invalid response")
			}

			key := parts[1]
			// check key length
			length, e2 := strconv.Atoi(parts[2])
			if e2 != nil {
				return errors.New("invalid response")
			}
			if length > MaxBodyLength {
				return errors.New("body too large")
			}

			item := &Item{}
			// FIXME
			if length > AllocLimit {
				item.alloc = cmem.Alloc(uintptr(length))
				item.Body = (*[1 << 30]byte)(unsafe.Pointer(item.alloc))[:length]
				(*reflect.SliceHeader)(unsafe.Pointer(&item.Body)).Cap = length
				runtime.SetFinalizer(item, func(item *Item) {
					if item.alloc != nil {
						//log.Print("free by finalizer: ", cap(item.Body))
						cmem.Free(item.alloc, uintptr(cap(item.Body)))
						item.Body = nil
						item.alloc = nil
					}
				})
			} else {
				item.Body = make([]byte, length)
			}
			if _, e = io.ReadFull(b, item.Body); e != nil {
				return e
			}
			b.ReadByte() // \r
			b.ReadByte() // \n
			resp.items[key] = item
			continue

		case "STAT":
			if len(parts) != 3 {
				return errors.New("invalid response")
			}
			var item Item
			item.Body = []byte(parts[2])
			resp.items[parts[1]] = &item
			continue

		case "END":
		case "STORED", "NOT_STORED", "DELETED", "NOT_FOUND":
		case "OK":

		case "ERROR", "SERVER_ERROR", "CLIENT_ERROR":
			if len(parts) > 1 {
				resp.msg = parts[1]
			}
			log.Print("error:", resp)

		default:
			// try to convert to int
			_, err := strconv.Atoi(resp.status)
			if err != nil {
				log.Print("unknown status:", s, resp.status)
				return errors.New("unknown response:" + resp.status)
			}
		}
		break
	}
	return nil
}

func (resp *Response) Write(w io.Writer) error {
	if resp.noreply {
		return nil
	}

	switch resp.status {
	case "VALUE":
		for key, item := range resp.items {
			fmt.Fprintf(w, "VALUE %s %d\r\n", key,
				len(item.Body))
			if e := WriteFull(w, item.Body); e != nil {
				return e
			}
			WriteFull(w, []byte("\r\n"))
		}
		io.WriteString(w, "END\r\n")

	case "STAT":
		io.WriteString(w, resp.msg)
		io.WriteString(w, "END\r\n")

	default:
		io.WriteString(w, resp.status)
		if resp.msg != "" {
			io.WriteString(w, " "+resp.msg)
		}
		io.WriteString(w, "\r\n")
	}
	return nil
}

func (resp *Response) CleanBuffer() {
	for _, item := range resp.items {
		if item.alloc != nil {
			cmem.Free(item.alloc, uintptr(cap(item.Body)))
			item.alloc = nil
		}
		runtime.SetFinalizer(item, nil)
	}
	resp.items = nil
}

func writeLine(w io.Writer, s string) {
	io.WriteString(w, s)
	io.WriteString(w, "\r\n")
}

func (req *Request) Process(store Storage, stat *Stats) (resp *Response) {
	resp = new(Response)
	resp.noreply = req.NoReply

	switch req.Cmd {

	case "get":
		if len(req.Key) > MaxKeyLength {
			resp.status = "CLIENT_ERROR"
			resp.msg = "key too long"
			return resp
		}

		resp.status = "VALUE"

		stat.cmd_get++
		key := req.Key
		item, err := store.Get(key)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			return resp
		}
		if item == nil {
			stat.get_misses++
		} else {
			resp.items = make(map[string]*Item, 1)
			resp.items[key] = item
			stat.get_hits++
			stat.bytes_written += int64(len(item.Body))
		}

	case "set":
		key := req.Key
		suc, err := store.Set(key, req.Item, req.NoReply)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			break
		}

		stat.cmd_set++
		stat.bytes_read += int64(len(req.Item.Body))
		if suc {
			resp.status = "STORED"
		} else {
			resp.status = "NOT_STORED"
		}

	case "delete":
		key := req.Key
		suc, err := store.Delete(key)
		if err != nil {
			resp.status = "SERVER_ERROR"
			resp.msg = err.Error()
			break
		}
		if suc {
			resp.status = "DELETED"
		} else {
			resp.status = "NOT_FOUND"
		}
		stat.cmd_delete++

	case "stats":
		st := stat.Stats()
		n := int64(store.Len())
		st["curr_items"] = n
		st["total_items"] = n
		resp.status = "STAT"
		var ss []string
		ss = make([]string, len(st))
		cnt := 0
		for k, v := range st {
			ss[cnt] = fmt.Sprintf("STAT %s %d\r\n", k, v)
			cnt += 1
		}
		resp.msg = strings.Join(ss, "")

	case "version":
		resp.status = "VERSION"
		resp.msg = VERSION

	case "verbosity", "flush_all":
		store.FlushAll()
		resp.status = "OK"

	case "quit":
		return nil

	default:
		// client error
		return nil
		resp.status = "CLIENT_ERROR"
		resp.msg = "invalid cmd"
	}
	return resp
}

func contain(vs []string, v string) bool {
	for _, i := range vs {
		if i == v {
			return true
		}
	}
	return false
}

func (req *Request) Check(resp *Response) error {
	switch req.Cmd {
	case "get":
		if resp.items != nil {
			for key, _ := range resp.items {
				if req.Key != key {
					log.Print("unexpected key in response: ", key)
					return errors.New("unexpected key in response: " + key)
				}
			}
		}

	case "set":
		if !contain([]string{"STORED", "NOT_STORED", "EXISTS", "NOT_FOUND"},
			resp.status) {
			return errors.New("unexpected status: " + resp.status)
		}
	}
	return nil
}
