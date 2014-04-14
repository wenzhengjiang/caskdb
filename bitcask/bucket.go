package bitcask

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

const (
	RECORD_HEADER_SIZE int32 = 20
)

/*
    Each record is stored in the following format:
   	|-----------------------------------------------------------------------------------------|
	|crc (uint32) | tstamp (int64) | ksz(int32) | vsz (int32) | key ([]byte) | value ([]byte) |
	|-----------------------------------------------------------------------------------------|
*/

type Record struct {
	crc    uint32
	tstamp int64
	ksz    int32
	vsz    int32
	key    []byte
	value  []byte
}

/*
   File wraps *os.File with read and write records, compression, buf
*/
type Bucket struct {
	io     *os.File
	wbuf   *bufio.Writer
	offset int32
	id     int32
}

func NewBucket(f *os.File, id int32) *Bucket {
	Lg.Println("Create bucket " + f.Name())
	offset, _ := f.Seek(0, 1)
	return &Bucket{
		io:     f,
		wbuf:   bufio.NewWriter(f),
		offset: int32(offset),
		id:     id}
}

func (b *Bucket) Write(key string, value []byte, tstamp int64) (int32, error) {
	r := &Record{
		key:    []byte(key),
		value:  value,
		ksz:    int32(len(key)),
		vsz:    int32(len(value)),
		tstamp: tstamp,
	}
	var (
		pos int32
		err error
	)
	if pos, err = b.writeRecord(r); err != nil {
		return -1, err
	}
	return pos, nil
}

func (b *Bucket) Sync() error {
	if err := b.wbuf.Flush(); err != nil {
		return err
	}
	e := b.io.Sync()
	return e
}

func (b *Bucket) Read() (*Record, error) {
	// 1. read header
	// 2. get ksz and vsz and read key and value
	r := new(Record)
	headerData := make([]byte, RECORD_HEADER_SIZE)
	var (
		sz  int
		err error
	)
	if sz, err = b.io.Read(headerData); err != nil {
		return nil, err
	}
	if int32(sz) != RECORD_HEADER_SIZE {
		return nil, fmt.Errorf("Read Header: exptectd %d, got %d", RECORD_HEADER_SIZE, sz)
	}
	buf := bytes.NewReader(headerData)
	binary.Read(buf, binary.BigEndian, &r.crc)
	binary.Read(buf, binary.BigEndian, &r.tstamp)
	binary.Read(buf, binary.BigEndian, &r.ksz)
	binary.Read(buf, binary.BigEndian, &r.vsz)
	r.key = make([]byte, r.ksz)
	r.value = make([]byte, r.vsz)
	if _, err := b.io.Read(r.key); err != nil {
		return nil, fmt.Errorf("key: %s", err.Error())
	}
	if _, err := b.io.Read(r.value); err != nil {
		return nil, fmt.Errorf("Read value: %s", err.Error())
	}
	// 3. check crc
	data := append(append(headerData, r.key...), r.value...)
	crc := crc32.ChecksumIEEE(data[4:])
	if r.crc != crc {
		return nil, fmt.Errorf("CRC check failed %u %u 2335539323", r.crc, crc)
	}
	return r, err
}

func (f *Bucket) Name() string {
	if f.io != nil {
		return f.io.Name()
	}
	return ""
}

func (f *Bucket) Close() error {
	if err := f.wbuf.Flush(); err != nil {
		return err
	}
	return f.io.Close()
}

/*************** Private Functions *******************/
//1. encode record to []byte
//2. write
//3. update current offset

func (b *Bucket) writeRecord(r *Record) (int32, error) {

	data, err := r.encode()
	if err != nil {
		return -1, err
	}
	sz, err := b.wbuf.Write(data)
	if err != nil {
		return -1, err
	}
	if sz < len(data) {
		err = fmt.Errorf("writeRecord: expected %d got %d\n", len(data), sz)
		return -1, err
	}
	vpos := int32(b.offset + RECORD_HEADER_SIZE /* crc + tstamp + ksz + vsz */ + int32(len(r.key)))
	b.offset += int32(sz)
	Lg.Println("write %s to %s", string(r.key), b.io.Name())
	return vpos, nil
}
func (r *Record) compress() error {
	var b bytes.Buffer
	w := zlib.NewWriter(&b)
	if _, err := w.Write(r.value); err != nil {
		return fmt.Errorf("compress value : %s", err.Error())
	}
	w.Close()
	r.value = []byte(b.Bytes())
	r.vsz = int32(len(r.value))
	return nil
}

func (r *Record) uncompress() error {
	b := bytes.NewReader(r.value)
	zr, err := zlib.NewReader(b)
	if err != nil {
		return fmt.Errorf("uncompress value %s", err.Error())
	}
	var buf bytes.Buffer
	_, err = io.Copy(&buf, zr)
	r.value = buf.Bytes()
	zr.Close()
	return nil
}

// 1.compress value(optional)
// 2.convert to binary format
// 3. calculate crc
func (r *Record) encode() ([]byte, error) {

	//    TODO why this is wrong ?
	//    headerData := make([]byte, RECORD_HEADER_SIZE)
	//    buf := bytes.NewReader(headerData[4:])
	//    binary.Read(buf, binary.BigEndian, int32(0))
	//    binary.Read(buf, binary.BigEndian, int32(1))
	//    binary.Read(buf, binary.BigEndian, int32(4))
	//    binary.Read(buf, binary.BigEndian, int32(6))
	//    data := append(append(headerData, []byte("key1")...), []byte("value1")...)
	//    crc := crc32.ChecksumIEEE(data[4:])
	//    binary.BigEndian.PutUint32(data, uint32(crc))
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, r.tstamp)
	binary.Write(buf, binary.BigEndian, r.ksz)
	binary.Write(buf, binary.BigEndian, r.vsz)
	buf.Write(r.key)
	buf.Write(r.value)
	crc := crc32.ChecksumIEEE(buf.Bytes())

	buf2 := new(bytes.Buffer)
	binary.Write(buf2, binary.BigEndian, crc)
	buf2.Write(buf.Bytes())

	return buf2.Bytes(), nil
}
