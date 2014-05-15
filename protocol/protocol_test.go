package protocol

import (
	"bufio"
	"bytes"
	"fmt"
	"strings"
	"testing"
)

type reqTest struct {
	cmd    string
	anwser string
}

var reqTests = []reqTest{
	reqTest{
		"get   \r\n",
		"CLIENT_ERROR invalid cmd\r\n",
	},
	reqTest{
		"get  " + strings.Repeat("a", 300) + " \r\n",
		"CLIENT_ERROR key too long\r\n",
	},
	reqTest{
		"set cdf 2\r\nok\r\n",
		"STORED\r\n",
	},
	reqTest{
		"get cdf\r\n",
		"VALUE cdf 2\r\nok\r\nEND\r\n",
	},
	reqTest{
		"set abc 1\r\nd\r\n",
		"STORED\r\n",
	},
	reqTest{
		"delete abc\r\n",
		"DELETED\r\n",
	},

	reqTest{
		"set n 1\r\n5\r\n",
		"STORED\r\n",
	},

	reqTest{
		"quit\r\n",
		"",
	},
	reqTest{
		"error\r\n",
		"CLIENT_ERROR unknown command: error\r\n",
	},
}

func TestRequest(t *testing.T) {
	store := NewMapStore()
	stats := NewStats()

	for i, test := range reqTests {
		buf := bytes.NewBufferString(test.cmd)
		req := new(Request)
		e := req.Read(bufio.NewReader(buf))
		var resp *Response
		if e != nil {
			resp = &Response{status: "CLIENT_ERROR", msg: e.Error()}
		} else {
			resp = req.Process(store, stats)
		}

		r := make([]byte, 0)
		wr := bytes.NewBuffer(r)
		if resp != nil {
			resp.Write(wr)
		}
		ans := wr.String()
		if test.anwser != ans {
			fmt.Print(req, resp)
			t.Errorf("test %d: expect %s[%d], bug got %s[%d]\n", i,
				test.anwser, len(test.anwser), ans, len(ans))
		}
	}
}
