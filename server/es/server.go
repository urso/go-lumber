package es

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"

	"github.com/klauspost/compress/gzip"

	"github.com/elastic/go-lumber/lj"
)

type Server struct {
	l       net.Listener
	handler httpHandler
	http    *http.Server
}

type httpHandler struct {
	silent bool
	split  int

	ownCh bool
	ch    chan *lj.Batch
}

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
	server.handler.split = cfg.split
	server.handler.silent = cfg.silent
	server.handler.ch = cfg.ch
	if cfg.ch == nil {
		server.handler.ch = make(chan *lj.Batch, 256)
		server.handler.ownCh = true
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
	type info struct {
		batch *lj.Batch
		meta  []map[string]interface{}
	}

	var writer io.Writer = resp
	if strings.Contains(requ.Header.Get("Accept-Encoding"), "gzip") {
		gz, _ := gzip.NewWriterLevel(writer, 3)
		defer gz.Close()
		writer = gz
		resp.Header().Add("Content-Encoding", "gzip")
	}

	var reader io.Reader = requ.Body
	if requ.Header.Get("Content-Encoding") == "gzip" {
		var err error
		reader, err = gzip.NewReader(reader)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			resp.Header().Add("Content-Type", "text/plain")
			writer.Write([]byte(err.Error()))
			return
		}
	}

	decoder := json.NewDecoder(reader)
	metas := make([]map[string]interface{}, 0, h.split)
	events := make([]interface{}, 0, h.split)
	batches := make(chan info, 2)
	var wg sync.WaitGroup
	wg.Add(1)

	resp.Header().Add("Content-Type", "application/json")
	resp.WriteHeader(http.StatusOK)
	writer.Write([]byte(`{"items": [`))

	go func() {
		defer wg.Done()
		first := true
		for info := range batches {
			batch := info.batch
			h.ch <- batch

			if h.silent {
				continue
			}

			if len(info.meta) == 0 {
				continue
			}

			i := 0
			if first {
				writer.Write([]byte(`{"created":{"status": 200}}`))
				first = false
				i = 1
			}
			for ; i < len(info.meta); i++ {
				writer.Write([]byte(`,{"created":{"status": 200}}`))
			}

			<-batch.Await()
			// optional
			// write ACK response
		}
	}()

	var err error
	for {
		if !decoder.More() {
			break
		}

		var meta map[string]interface{}
		var evt map[string]interface{}

		err = decoder.Decode(&meta)
		if err != nil {
			break
		}

		err = decoder.Decode(&evt)
		if err != nil {
			break
		}

		evt["@metadata"] = meta
		events = append(events, evt)
		metas = append(metas, meta)
		if len(events) == cap(events) {
			batches <- info{lj.NewBatch(events), metas}
			metas = make([]map[string]interface{}, 0, h.split)
			events = make([]interface{}, 0, h.split)
		}
	}

	if len(events) > 0 {
		batches <- info{lj.NewBatch(events), metas}
	}

	close(batches)
	wg.Wait()

	writer.Write([]byte("]}"))
}
