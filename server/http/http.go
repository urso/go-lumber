package http

import (
	"net"
	"net/http"
	"time"

	"github.com/elastic/go-lumber/lj"
	"github.com/elastic/go-lumber/server/internal"
	"github.com/elastic/go-lumber/server/v2"
)

type Server struct {
	l       net.Listener
	handler httpHandler
	http    *http.Server
}

type httpHandler struct {
	ownCh bool
	ch    chan *lj.Batch

	opts     options
	versions map[string]func(net.Conn) (internal.BatchReader, internal.ACKWriter, error)
}

type handlerConn struct {
	requ *http.Request
	resp http.ResponseWriter
}

type chunkedACKWriter struct {
	w       internal.ACKWriter
	flusher http.Flusher
}

type urlString string

func NewWithListener(l net.Listener, opts ...Option) (*Server, error) {
	return newServer(l, "", opts)
}

func ListenAndServeWith(
	binder func(network, addr string) (net.Listener, error),
	addr string,
	opts ...Option,
) (*Server, error) {
	l, err := binder("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewWithListener(l, opts...)
}

func ListenAndServe(addr string, opts ...Option) (*Server, error) {
	return newServer(nil, addr, opts)
}

func newServer(l net.Listener, addr string, opts []Option) (*Server, error) {
	cfg, err := applyOptions(opts)
	if err != nil {
		return nil, err
	}

	if addr == "" {
		addr = ":http"
	}

	server := &Server{l: l}
	server.handler.ch = cfg.ch
	if cfg.ch == nil {
		server.handler.ch = make(chan *lj.Batch, 256)
		server.handler.ownCh = true
	}
	server.handler.opts = cfg
	server.handler.versions = map[string]func(net.Conn) (internal.BatchReader,
		internal.ACKWriter, error){
		"2.0": v2.MakeIOHandler(cfg.timeout, cfg.decoder),
	}

	http := &http.Server{
		Addr:         addr,
		Handler:      &server.handler,
		ReadTimeout:  cfg.timeout,
		WriteTimeout: cfg.timeout,
		TLSConfig:    cfg.tls,
		ErrorLog:     nil, // TODO
	}

	server.http = http
	switch {
	case l != nil:
		go http.Serve(l)
	case cfg.tls != nil:
		go http.ListenAndServeTLS("", "")
	default:
		go http.ListenAndServe()
	}

	return server, nil
}

func (s *Server) Close() error {
	if s.l != nil {
		return s.l.Close()
	}
	return nil
}

func (s *Server) Receive() *lj.Batch {
	return <-s.ReceiveChan()
}

func (s *Server) ReceiveChan() <-chan *lj.Batch {
	return s.handler.ch
}

func (h *httpHandler) ServeHTTP(resp http.ResponseWriter, requ *http.Request) {
	switch requ.Method {
	case "HEAD": // ping request
		resp.WriteHeader(http.StatusOK)
	case "POST": // bulk send request
		h.serveBulk(resp, requ)
	default: // unknown request
		resp.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *httpHandler) serveBulk(resp http.ResponseWriter, requ *http.Request) {
	version := requ.Header.Get("X-Lumberjack-Version")
	if version == "" {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	handler := h.versions[version]
	if handler == nil {
		resp.WriteHeader(http.StatusBadRequest)
		return
	}

	conn := &handlerConn{requ, resp}
	ljReader, ljWriter, err := handler(conn)
	if err != nil {
		resp.WriteHeader(http.StatusServiceUnavailable)
		resp.Header().Add("Content-type", "text/plain")
		resp.Write([]byte(err.Error()))
		return
	}

	hasKeepalive := false
	if h.opts.keepalive > 0 && ljWriter.HasKeepalive() {
		flusher, ok := resp.(http.Flusher)
		if ok {
			hasKeepalive = true
			ljWriter = &chunkedACKWriter{ljWriter, flusher}
		}
	}

	batch, err := ljReader.ReadBatch()
	if err != nil {
		resp.WriteHeader(http.StatusServiceUnavailable)
		resp.Header().Add("Content-type", "text/plain")
		resp.Write([]byte(err.Error()))
		return
	}
	N := len(batch.Events)
	h.ch <- batch

	resp.Header().Add("Content-Type", "application/lumberjack")
	resp.Header().Add("X-Lumberjack-Version", version)

	if hasKeepalive {
		hasACK := false
		for !hasACK {
			select {
			case <-batch.Await():
				hasACK = true
			case <-time.After(h.opts.keepalive):
				if err := ljWriter.Keepalive(0); err != nil {
					return
				}
			}
		}
	} else {
		<-batch.Await()
	}
	ljWriter.ACK(N)
}

func (c *handlerConn) Write(b []byte) (int, error) {
	n, err := c.resp.Write(b)
	return n, err
}

func (c *handlerConn) Read(b []byte) (int, error) {
	return c.requ.Body.Read(b)
}

func (c *handlerConn) Close() error {
	c.requ.Body.Close()
	return nil
}

func (c *handlerConn) LocalAddr() net.Addr {
	return urlString(c.requ.Host)
}

func (c *handlerConn) RemoteAddr() net.Addr {
	return urlString(c.requ.RemoteAddr)
}

func (c *handlerConn) SetDeadline(t time.Time) error      { return nil }
func (c *handlerConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *handlerConn) SetWriteDeadline(t time.Time) error { return nil }

func (u urlString) String() string  { return string(u) }
func (u urlString) Network() string { return string(u) }

func (c *chunkedACKWriter) HasKeepalive() bool {
	return c.w.HasKeepalive()
}

func (c *chunkedACKWriter) Keepalive(n int) error {
	err := c.w.Keepalive(n)
	if err != nil {
		c.flusher.Flush()
	}
	return err
}

func (c *chunkedACKWriter) ACK(n int) error {
	return c.w.ACK(n)
}
