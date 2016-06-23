package v2

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

type HttpClient struct {
	conn   *httpConn
	client *Client
}

type httpConn struct {
	http     *http.Client
	url      urlString
	username string
	password string

	buf  *bytes.Buffer
	resp *http.Response

	canceler chan struct{}
}

type urlString string

func NewHTTPClient(
	url string,
	username, password string,
	transp *http.Transport,
	opts ...Option,
) (*HttpClient, error) {
	o, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}

	conn := &httpConn{
		url:      urlString(url),
		username: "",
		password: "",
		http: &http.Client{
			Transport: transp,
			Timeout:   o.timeout,
		},
		buf:      bytes.NewBuffer(nil),
		canceler: make(chan struct{}, 1),
	}

	client, err := NewWithConn(conn, opts...)
	if err != nil {
		return nil, err
	}

	c := &HttpClient{
		conn:   conn,
		client: client,
	}
	return c, nil
}

func (c *HttpClient) Send(data []interface{}) (int, error) {
	c.conn.Reset()

	// create http body
	if err := c.client.Send(data); err != nil {
		return 0, err
	}

	// send http request
	err := c.conn.Push()
	if err != nil {
		return 0, err
	}

	// wait for ACK of batch
	n, err := c.client.AwaitACK(uint32(len(data)))
	return int(n), err
}

func (c *httpConn) Reset() {
	c.buf.Reset()
}

func (c *httpConn) Push() error {
	requ, err := http.NewRequest("POST", c.url.String(), c.buf)
	if err != nil {
		return err
	}

	requ.Cancel = c.canceler
	if c.username != "" && c.password != "" {
		requ.SetBasicAuth(c.username, c.password)
	}

	requ.Header.Add("Content-Type", "application/lumberjack")
	requ.Header.Add("Accept", "application/lumberjack")
	requ.Header.Add("X-Lumberjack-Version", "2.0")

	resp, err := c.http.Do(requ)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP endpoint returned status '%v'", resp.Status)
	}

	c.resp = resp
	return nil
}

func (c *httpConn) Write(b []byte) (n int, err error) {
	return c.buf.Write(b)
}

func (c *httpConn) Read(b []byte) (int, error) {
	if c.resp == nil {
		return 0, errors.New("No HTTP Response")
	}

	n, err := c.resp.Body.Read(b)
	if err == io.EOF {
		c.resp.Body.Close()
		c.resp = nil
		err = nil
	}
	return n, err
}

func (c *httpConn) Close() error {
	c.canceler <- struct{}{}
	return nil
}

func (c *httpConn) LocalAddr() net.Addr {
	return urlString("local")
}

func (c *httpConn) RemoteAddr() net.Addr {
	return c.url
}

func (c *httpConn) SetDeadline(t time.Time) error      { return nil }
func (c *httpConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *httpConn) SetWriteDeadline(t time.Time) error { return nil }

func (u urlString) String() string  { return string(u) }
func (u urlString) Network() string { return string(u) }
