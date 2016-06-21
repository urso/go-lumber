// Lumberjack server test tool.
//
// Create lumberjack server endpoint ACKing all received batches only. The
// server supports all lumberjack protocol versions, which must be explicitely enabled
// from command line. For printing list of known command line flags run:
//
//  tst-lj -h
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/elastic/go-lumber/server"
)

type rateLimiter struct {
	ticker *time.Ticker
	ch     chan time.Time
}

func main() {
	bind := flag.String("bind", ":5044", "[host]:port to listen on")
	v1 := flag.Bool("v1", false, "Enable protocol version v1")
	v2 := flag.Bool("v2", false, "Enable protocol version v2")
	limit := flag.Int("rate", 0, "max batch ack rate")
	es := flag.Bool("es", false, "Enable simplified es bulk protocol")
	esSplit := flag.Int("split", 2048, "Batch split limit for es bulk events")
	esSilent := flag.Bool("silent", false, "If enabled send empty response")
	quiet := flag.Bool("q", false, "Quiet")

	flag.Parse()

	s, err := server.ListenAndServe(*bind,
		server.V1(*v1),
		server.V2(*v2),
		server.ES(*es),
		server.Split(*esSplit),
		server.Silent(*esSilent),
	)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	log.Println("tcp server up")

	var rl *rateLimiter
	if *limit > 0 {
		rl = newRateLimiter(*limit, (*limit)*2, time.Second)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill)
	go func() {
		<-sig
		if rl != nil {
			rl.Stop()
		}
		_ = s.Close()
		os.Exit(0)
	}()

	debug := !*quiet
	if rl == nil {
		log.Println("no rate limit")
		for batch := range s.ReceiveChan() {
			if debug {
				log.Printf("Received batch of %v events\n", len(batch.Events))
			}
			batch.ACK()
		}
	} else {
		log.Println("rate limit: ", *limit)
		for batch := range s.ReceiveChan() {
			if !rl.Wait() {
				break
			}

			if debug {
				log.Printf("Received batch of %v events\n", len(batch.Events))
			}
			batch.ACK()
		}
	}
}

func newRateLimiter(limit, burstLimit int, unit time.Duration) *rateLimiter {
	interval := time.Duration(uint64(unit) / uint64(limit))
	ticker := time.NewTicker(interval)
	ch := make(chan time.Time, burstLimit)
	r := &rateLimiter{ticker: ticker, ch: ch}

	go func() {
		defer close(ch)
		for t := range ticker.C {
			ch <- t
		}
	}()

	return r
}

func (r *rateLimiter) Stop() {
	r.ticker.Stop()
}

func (r *rateLimiter) Wait() bool {
	_, ok := <-r.ch
	return ok
}
