package client

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

// small-scale localhost test for sincronia library and scheduler

func runScheduler(
	c context.Context,
	ready chan interface{},
	done chan struct{},
) {
	killer := exec.Command("killall", "sincronia-scheduler")
	killer.Run()
	ctx, cancel := context.WithCancel(c)
	logF, err := os.Create("scheduler-test.log")
	cmd := exec.CommandContext(ctx, "../scheduler/sincronia-scheduler")
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

func setup() chan struct{} {
	done := make(chan struct{})
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGTERM)

		<-c
		done <- struct{}{}
	}()
	ready := make(chan interface{})
	go runScheduler(context.Background(), ready, done)
	<-ready

	return done
}

// steps
// 1. start scheduler
// 2. start application master
func TestBasic(t *testing.T) {
	done := setup()

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
					Blob:   bytes.NewReader([]byte{'h', 'e', 'l', 'l', 'o'}),
				},
			},
			Flow{
				JobID: 1,
				From:  3,
				To:    4,
				Info: Data{
					DataID: 1,
					Size:   5,
					Blob:   bytes.NewReader([]byte{'h', 'e', 'l', 'l', 'o'}),
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
					Blob:   bytes.NewReader([]byte{'h', 'e', 'l', 'l', 'o'}),
				},
			},
			Flow{
				JobID: 2,
				From:  2,
				To:    4,
				Info: Data{
					DataID: 3,
					Size:   5,
					Blob:   bytes.NewReader([]byte{'a', 'g', 'a', 'i', 'n'}),
				},
			},
		},
	}

	<-time.After(5 * time.Millisecond)
	appMaster("127.0.0.1:16424", []Coflow{cf1, cf2})
	done <- struct{}{}
}

// two coflows on two nodes, all flows from 1 to 2
func TestOneDirection(t *testing.T) {
	done := setup()

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
					Blob:   bytes.NewReader([]byte{'h', 'e', 'l', 'l', 'o'}),
				},
			},
			Flow{
				JobID: 1,
				From:  1,
				To:    2,
				Info: Data{
					DataID: 3,
					Size:   5,
					Blob:   bytes.NewReader([]byte{'h', 'e', 'l', 'l', 'o'}),
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
				To:    2,
				Info: Data{
					DataID: 1,
					Size:   5,
					Blob:   bytes.NewReader([]byte{'a', 'g', 'a', 'i', 'n'}),
				},
			},
			Flow{
				JobID: 2,
				From:  1,
				To:    2,
				Info: Data{
					DataID: 2,
					Size:   3,
					Blob:   bytes.NewReader([]byte{'f', 'o', 'o'}),
				},
			},
		},
	}
	<-time.After(5 * time.Millisecond)
	appMaster("127.0.0.1:16424", []Coflow{cf1, cf2})
	done <- struct{}{}
}

// Two coflows on two nodes, exchanging data
func TestCrossing(t *testing.T) {
	done := setup()

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
					Size:   6,
					Blob:   bytes.NewReader([]byte{'h', 'e', 'l', 'l', 'o', '!'}),
				},
			},
			//Flow{
			//	JobID: 1,
			//	From:  2,
			//	To:    1,
			//	Info: Data{
			//		DataID: 1,
			//		Size:   5,
			//		Blob:   []byte{'a', 'g', 'a', 'i', 'n'},
			//	},
			//},
		},
	}

	cf2 := Coflow{
		JobID: 2,
		Flows: []Flow{
			Flow{
				JobID: 2,
				From:  1,
				To:    2,
				Info: Data{
					DataID: 2,
					Size:   4,
					Blob:   bytes.NewReader([]byte{'b', 'e', 'a', 'r'}),
				},
			},
			Flow{
				JobID: 2,
				From:  2,
				To:    1,
				Info: Data{
					DataID: 3,
					Size:   3,
					Blob:   bytes.NewReader([]byte{'f', 'o', 'o'}),
				},
			},
		},
	}

	<-time.After(5 * time.Millisecond)
	appMaster("127.0.0.1:16424", []Coflow{cf1, cf2})
	done <- struct{}{}
}

func TestSpecial(t *testing.T) {
	done := setup()

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
					Blob:   bytes.NewReader([]byte{'h', 'e', 'l', 'l', 'o'}),
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
				To:    2,
				Info: Data{
					DataID: 1,
					Size:   5,
					Blob:   bytes.NewReader([]byte{'a', 'g', 'a', 'i', 'n'}),
				},
			},
			Flow{
				JobID: 2,
				From:  2,
				To:    1,
				Info: Data{
					DataID: 2,
					Size:   3,
					Blob:   bytes.NewReader([]byte{'f', 'o', 'o'}),
				},
			},
		},
	}
	cf3 := Coflow{
		JobID: 3,
		Flows: []Flow{
			Flow{
				JobID: 3,
				From:  1,
				To:    2,
				Info: Data{
					DataID: 3,
					Size:   5,
					Blob:   bytes.NewReader([]byte{'a', 'g', 'a', 'i', 'n'}),
				},
			},
		},
	}
	cf4 := Coflow{
		JobID: 4,
		Flows: []Flow{
			Flow{
				JobID: 4,
				From:  1,
				To:    2,
				Info: Data{
					DataID: 4,
					Size:   5,
					Blob:   bytes.NewReader([]byte{'a', 'g', 'a', 'i', 'n'}),
				},
			},
		},
	}

	<-time.After(5 * time.Millisecond)
	appMaster("127.0.0.1:16424", []Coflow{cf1, cf2, cf3, cf4})
	done <- struct{}{}
}

// will give you as much junk data as you request
type infRead struct{}

func (i infRead) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte('a')
	}

	return len(p), nil
}

func TestLargeFlow(t *testing.T) {
	done := setup()

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
					Size:   1e7,
					Blob:   io.LimitReader(infRead{}, 1e7),
				},
			},
		},
	}

	<-time.After(5 * time.Millisecond)
	appMaster("127.0.0.1:16424", []Coflow{cf1})
	done <- struct{}{}
}

func TestLargeFlowOddSize(t *testing.T) {
	done := setup()

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
					Size:   1234567,
					Blob:   io.LimitReader(infRead{}, 1234567),
				},
			},
		},
	}

	<-time.After(5 * time.Millisecond)
	appMaster("127.0.0.1:16424", []Coflow{cf1})
	done <- struct{}{}
}

func sliceForClient(cf []Coflow, client uint32) map[uint32][]Flow {
	cfsl := make(map[uint32][]Flow)
	for _, c := range cf {
		cfsl[c.JobID] = make([]Flow, 0)
		for _, f := range c.Flows {
			if f.From == client {
				cfsl[c.JobID] = append(cfsl[c.JobID], f)
			}
		}
	}

	return cfsl
}

// launch coflows for 4 clients
// 1. coflows: [(1->2: 1MB, 3->4:1MB), (1->4:1MB, 2->4:1MB)]
// 2. define coflows and register with scheduler
// 3. start 4 clients
func appMaster(schedAddr string, cfs []Coflow) {
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

	s.RegCoflow(cfs)

	done := make(chan interface{})

	go client(schedAddr, 1, nodes, sliceForClient(cfs, 1), done)
	go client(schedAddr, 2, nodes, sliceForClient(cfs, 2), done)
	go client(schedAddr, 3, nodes, sliceForClient(cfs, 3), done)
	go client(schedAddr, 4, nodes, sliceForClient(cfs, 4), done)

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
						}).Info("flow incoming")

						go func(d Data) {
							sum := 0
							for chunk := range d.Recv {
								sum += len(chunk)
							}

							if sum != int(d.Size) {
								log.WithFields(log.Fields{
									"node":       id,
									"dataid":     d.DataID,
									"size":       d.Size,
									"recvd size": sum,
								}).Panic("flow size mismatch")
							}

							log.WithFields(log.Fields{
								"node":   id,
								"dataid": d.DataID,
								"size":   d.Size,
							}).Info("flow done")
						}(r)
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
	}

	log.WithFields(log.Fields{
		"node": id,
	}).Info("client done")
	s.Stop()
	done <- struct{}{}
}
