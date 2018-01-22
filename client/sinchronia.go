package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"./scheduler"

	log "github.com/sirupsen/logrus"
	"zombiezen.com/go/capnproto2/rpc"
)

// Data is the external type for a piece of data to transfer
// The application must assign each piece of data a unique id.
type Data struct {
	DataID uint32
	Size   uint32
	Blob   []byte // can be un-set for registration
}

// Flow is the transfer of a Data
type Flow struct {
	JobID uint32
	From  uint32
	To    uint32
	Info  Data
}

// Coflow is a full view of a coflow, for registration
type Coflow struct {
	JobID uint32
	Flows []Flow
}

type coflowScheduleItem struct {
	jobID    uint32
	priority uint32
}

// CoflowSlice is a client's view of a coflow
type coflowSlice struct {
	jobID  uint32
	nodeID uint32
	send   map[uint32]sendFlow
	recv   map[uint32]Data

	incoming chan Flow
	ret      chan Data // return received Data to SendCoflow caller over this channel
}

// Sinchronia manages interactions with the coflow scheduler
// A "Node" is something which sends or receives flows belonging to coflows.
// The application calling this library should give each Node a unique id.
type Sinchronia struct {
	NodeID      uint32
	schedClient scheduler.Scheduler
	ctx         context.Context
	nodeMap     map[uint32]string
	recv        chan Flow
	newCoflow   chan coflowSlice
	stop        chan struct{}
}

// Sending Loop
// 1. Poll the master for the CoflowSchedule
// 2. Of all the local coflows, send the highest priority one
func (s *Sinchronia) outgoing(newCf chan coflowSlice) {
	var err error
	coflows := make(map[uint32]coflowSlice)
	var currSchedule []coflowScheduleItem
	var mu sync.Mutex

	go func() {
		// get any new coflows
		for cf := range newCf {
			mu.Lock()
			coflows[cf.jobID] = cf
			mu.Unlock()
		}
	}()

	for {
		// 1. Poll the master for the CoflowSchedule
		currSchedule, err = s.getSchedule()
		if err != nil {
			//log.WithFields(log.Fields{
			//	"node": s.NodeID,
			//	"err":  err,
			//}).Warn("GetSchedule RPC")
		}

		if len(currSchedule) == 0 {
			continue
		}

		// 2. Of all the local coflows, send the highest priority one
		for _, scheduled := range currSchedule {
			mu.Lock()
			currCf, ok := coflows[scheduled.jobID]
			mu.Unlock()
			if !ok {
				// this coflow already finished
				//mu.Lock()
				//log.WithFields(log.Fields{
				//	"node":         s.NodeID,
				//	"jobID":        scheduled.jobID,
				//	"priority":     scheduled.priority,
				//	"currSchedule": currSchedule,
				//	"coflows":      coflows,
				//}).Warn("scheduled coflow already finished")
				//mu.Unlock()
			} else {
				log.WithFields(log.Fields{
					"node":   s.NodeID,
					"currCf": currCf.jobID,
				}).Info("sending coflow")
				currCf.sendOneFlow(s, scheduled.priority)

				if len(currCf.send) == 0 {
					log.WithFields(log.Fields{
						"node":   s.NodeID,
						"currCf": currCf.jobID,
					}).Info("coflow done sending")
					mu.Lock()
					delete(coflows, currCf.jobID)
					mu.Unlock()
				}

				break
			}
		}
	}
}

// Receiving
// Distribute incoming flows across all coflows to the correct coflowSlice
func (s *Sinchronia) incoming(newCf chan coflowSlice) {
	coflows := make(map[uint32]coflowSlice)
	done := make(chan uint32)
	for {
		select {
		case cf := <-newCf:
			coflows[cf.jobID] = cf
			go s.recvExpected(cf, done)
		case f := <-s.recv:
			cf := coflows[f.JobID]
			cf.incoming <- f
		case jid := <-done:
			delete(coflows, jid)
		}
	}

}

func (s *Sinchronia) newCoflows() {
	outgoingCh := make(chan coflowSlice)
	go s.outgoing(outgoingCh)
	incomingCh := make(chan coflowSlice)
	go s.incoming(incomingCh)
	for cf := range s.newCoflow {
		if len(cf.send) > 0 {
			outgoingCh <- cf
		}

		if len(cf.recv) > 0 {
			incomingCh <- cf
		} else {
			close(cf.incoming)
			close(cf.ret)
		}
	}
}

// New returns a Sinchronia agent connected to the central scheduler
// schedAddr: central scheduler address
// nodeID: the unique id of this node
// nodeMap: a mapping of unique node ids to network addresses (i.e. 128.30.2.121:17000)
func New(schedAddr string, nodeID uint32, nodes map[uint32]string) (*Sinchronia, error) {
	conn, err := net.Dial("tcp4", schedAddr)
	if err != nil {
		return nil, fmt.Errorf("couldn't connect to coflow scheduler: %s", err)
	}

	err = conn.(*net.TCPConn).SetKeepAlive(true)
	if err != nil {
		return nil, fmt.Errorf("couldn't set KeepAlive on connection: %s", err)
	}

	ctx := context.Background()
	rpcConn := rpc.NewConn(rpc.StreamTransport(conn))
	sch := scheduler.Scheduler{Client: rpcConn.Bootstrap(ctx)}

	s := &Sinchronia{
		NodeID:      nodeID,
		schedClient: sch,
		ctx:         ctx,
		nodeMap:     nodes,
		recv:        make(chan Flow),
		newCoflow:   make(chan coflowSlice),
		stop:        make(chan struct{}),
	}

	go s.newCoflows()
	go listen(s.NodeID, s.nodeMap[s.NodeID], s.recv, s.stop)

	return s, nil
}

// Stop this instance of the agent
func (s *Sinchronia) Stop() {
	close(s.stop)
}

// RegCoflow called by application master to tell scheduler about all coflows.
// Only call this once per coflow.
// Coflow sizes need not be known in advance (set to 0)
func (s *Sinchronia) RegCoflow(cfs []Coflow) {
	s.schedClient.RegCoflow(
		s.ctx,
		func(
			p scheduler.Scheduler_regCoflow_Params,
		) error {
			pCfs, err := p.NewCoflows(int32(len(cfs)))
			if err != nil {
				log.Error("reg coflows", err)
				return err
			}

			for i, cf := range cfs {
				currCf := pCfs.At(i)
				currCf.SetJobID(cf.JobID)
				fls, err := currCf.NewFlows(int32(len(cf.Flows)))
				if err != nil {
					log.Error("reg coflows", err)
					return err
				}

				for i, f := range cf.Flows {
					currFl := fls.At(i)
					currFl.SetFrom(f.From)
					currFl.SetTo(f.To)
					d, err := currFl.NewData()
					if err != nil {
						log.Error("reg coflows", err)
						return err
					}

					d.SetDataID(f.Info.DataID)
					d.SetSize(f.Info.Size)
				}
			}

			return nil
		},
	).Struct()
}

// SendCoflow tells sinchronia to send the CoflowSlice.
// Only call this method once per coflow per node.
// When the central scheduler returns a coflow priority,
// the flow is automatically sent over the network.
//
// DO NOT start sending goroutines
//
// Returns a channel on which incoming flows are returned
func (s *Sinchronia) SendCoflow(
	jobID uint32,
	flows []Flow,
) (chan Data, error) {
	sendFlows := make(map[uint32]sendFlow)
	for _, fl := range flows {
		// make a channel for each flow
		sf := sendFlow{
			f:    fl,
			done: make(chan interface{}),
		}

		sendFlows[fl.Info.DataID] = sf
	}

	cf := coflowSlice{
		jobID:    jobID,
		nodeID:   s.NodeID,
		send:     sendFlows,
		incoming: make(chan Flow, 10),
		ret:      make(chan Data, 10),
	}

	go func() {
		cf.recv = s.coflowReady(cf)
		log.WithFields(log.Fields{
			"job":    cf.jobID,
			"node":   s.NodeID,
			"toSend": cf.send,
			"toRecv": cf.recv,
		}).Info("coflow is ready")
		s.newCoflow <- cf
	}()

	return cf.ret, nil
}

func after(ch chan interface{}) {
	<-time.After(100 * time.Millisecond)
	ch <- struct{}{}
}
