package client

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

// small-scale localhost test for sinchronia library and scheduler

// steps
// 1. start scheduler
// 2. start application master
func TestSinchronia(t *testing.T) {
	done := make(chan interface{})
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGTERM)

		<-c
		done <- struct{}{}
	}()
	ready := make(chan interface{})
	go runScheduler(context.Background(), ready, done)
	<-ready
	<-time.After(time.Millisecond)
	appMaster("127.0.0.1:16424")
}

func runScheduler(
	c context.Context,
	ready chan interface{},
	done chan interface{},
) {
	ctx, cancel := context.WithCancel(c)
	logF, err := os.Create("scheduler-test.log")
	cmd := exec.CommandContext(ctx, "../scheduler/sinchronia-scheduler")
	cmd.Stdout = logF
	cmd.Stderr = logF
	err = cmd.Start()
	if err != nil {
		panic(err)
	}

	<-time.After(time.Millisecond)
	ready <- struct{}{}
	<-done
	cancel()
}

// launch coflows for 4 clients
// 1. coflows: [(1->2: 1MB, 3->4:1MB), (1->4:1MB, 2->4:1MB)]
// 2. define coflows and register with scheduler
// 3. start 4 clients
func appMaster(schedAddr string) {
	// define clients
	// nodeID 0 = app master
	nodes := map[uint32]string{
		1: "127.0.0.1:17001",
		2: "127.0.0.1:17002",
		3: "127.0.0.1:17003",
		4: "127.0.0.1:17004",
	}

	s, err := New(schedAddr, 0, nodes)
	if err != nil {
		panic(err)
	}

	// define coflows
	cf1 := Coflow{
		JobID: 1,
		Flows: []Flow{
			Flow{
				JobID: 1,
				From:  1,
				To:    2,
				Info: Data{
					DataID: 0,
					Size:   5,
					Blob:   []byte{'h', 'e', 'l', 'l', 'o'},
				},
			},
			Flow{
				JobID: 1,
				From:  3,
				To:    4,
				Info: Data{
					DataID: 1,
					Size:   5,
					Blob:   []byte{'h', 'e', 'l', 'l', 'o'},
				},
			},
		},
	}

	cf2 := Coflow{
		JobID: 2,
		Flows: []Flow{
			Flow{
				JobID: 2,
				From:  1,
				To:    4,
				Info: Data{
					DataID: 2,
					Size:   5,
					Blob:   []byte{'h', 'e', 'l', 'l', 'o'},
				},
			},
			Flow{
				JobID: 2,
				From:  2,
				To:    4,
				Info: Data{
					DataID: 3,
					Size:   5,
					Blob:   []byte{'h', 'e', 'l', 'l', 'o'},
				},
			},
		},
	}

	s.RegCoflow([]Coflow{cf1, cf2})

	done := make(chan interface{})

	c1sl := map[uint32][]Flow{
		1: []Flow{cf1.Flows[0]},
		2: []Flow{cf2.Flows[0]},
	}
	go client(schedAddr, 1, nodes, c1sl, done)

	c2sl := map[uint32][]Flow{
		1: []Flow{},
		2: []Flow{cf2.Flows[1]},
	}
	go client(schedAddr, 2, nodes, c2sl, done)

	c3sl := map[uint32][]Flow{
		1: []Flow{cf1.Flows[1]},
		2: []Flow{},
	}
	go client(schedAddr, 3, nodes, c3sl, done)

	c4sl := map[uint32][]Flow{
		1: []Flow{},
		2: []Flow{},
	}
	go client(schedAddr, 4, nodes, c4sl, done)

	<-done
	<-done
	<-done
	<-done
	log.Info("appmaster() done")
}

func client(
	schedAddr string,
	id uint32,
	nodes map[uint32]string,
	cfs map[uint32][]Flow,
	done chan interface{},
) {
	s, err := New(schedAddr, id, nodes)
	if err != nil {
		panic(err)
	}

	cfsDone := make(chan interface{})
	for cf, fs := range cfs {
		log.WithFields(log.Fields{
			"node": id,
			"cf":   cf,
			"fs":   fs,
		}).Info("calling SendCoflow")

		rs, err := s.SendCoflow(cf, fs)
		if err != nil {
			panic(err)
		}

		go func(cf uint32, rcvs chan Data) {
		recv:
			for {
				select {
				case r, ok := <-rcvs:
					if !ok {
						break recv
					} else {
						log.WithFields(log.Fields{
							"node":   id,
							"dataid": r.DataID,
						}).Info("flow done")
					}
				case <-time.After(5 * time.Second):
					log.WithFields(log.Fields{
						"node": id,
						"job":  cf,
					}).Info("still listening")
				}
			}

			log.WithFields(log.Fields{
				"node": id,
				"cf":   cf,
			}).Info("done receiving")
			cfsDone <- struct{}{}
		}(cf, rs)
	}

	for _ = range cfs {
		<-cfsDone
		log.Info("cfsDone")
	}

	log.WithFields(log.Fields{
		"node": id,
	}).Info("client done")
	done <- struct{}{}
}
