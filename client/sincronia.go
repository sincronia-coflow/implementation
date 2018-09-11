package client

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/sincronia-coflow/implementation/client/scheduler"
	log "github.com/sirupsen/logrus"
	"zombiezen.com/go/capnproto2/rpc"
)

// Data is the external type for a piece of data to transfer
// The application must assign each piece of data a unique id.
// Blob is for outgoing application bytes, and Recv is for
// incoming received data.
type Data struct {
	DataID uint32
	Size   uint32
	Blob   io.Reader   // can be un-set for registration
	Recv   chan []byte // can be un-set for registration
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
	recv   map[uint32]*Data

	incoming chan Flow
	ret      chan Data // return received Data to SendCoflow caller over this channel
}

// Sincronia manages interactions with the coflow scheduler
// A "Node" is something which sends or receives flows belonging to coflows.
// The application calling this library should give each Node a unique id.
type Sincronia struct {
	NodeID      uint32
	schedClient scheduler.Scheduler
	ctx         context.Context
	nodeMap     map[uint32]string
	recv        chan Flow
	newCoflow   chan coflowSlice
	stop        chan struct{}
}

// send flows in 1 MB chunks
const flowChunkSizeBytes = 1000000

// Sending Loop
// 1. Poll the master for the CoflowSchedule
// 2. Of all the local coflows, send the highest priority one
func (s *Sincronia) outgoing(newCf chan coflowSlice) {
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
			log.WithFields(log.Fields{
				"node": s.NodeID,
				"err":  err,
			}).Debug("GetSchedule RPC")
			continue
		}

		if len(currSchedule) == 0 {
			continue
		}

		log.WithFields(log.Fields{
			"node":         s.NodeID,
			"currSchedule": currSchedule,
		}).Debug("got schedule")

		// 2. Of all the local coflows, send the highest priority one
		for _, scheduled := range currSchedule {
			mu.Lock()
			currCf, ok := coflows[scheduled.jobID]
			mu.Unlock()
			if ok {
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
func (s *Sincronia) incoming(newCf chan coflowSlice) {
	coflows := make(map[uint32]coflowSlice)
	orphanage := make(map[uint32][]Flow)
	done := make(chan uint32)
	for {
		select {
		case cf := <-newCf:
			coflows[cf.jobID] = cf
			go s.recvExpected(cf, done)

			// If this coflow lost the race with the sending side, then it has to collect
			// its orphaned flows from the orphanage.
			if orphans, ok := orphanage[cf.jobID]; ok {
				go func(fs []Flow) {
					log.WithFields(log.Fields{
						"job":  cf.jobID,
						"node": s.NodeID,
						// 						"coflows":   coflows,
						// 						"orphanage": orphanage,
					}).Info("collecting from orphanage")
					for _, orph := range fs {
						cf.incoming <- orph
					}

					delete(orphanage, cf.jobID)
				}(orphans)
			}
		case f := <-s.recv:
			cf, ok := coflows[f.JobID]

			// The notification to the receiver and sender in this coflow race.
			// If the sender gets the notification first, and is immediately scheduled,
			// then the flow can arrive at the receiver before the receiver is ready to receive it.
			// In this case, place the received flow(s) in the orphanage until its parent flow arrives
			// to collect it in cf.incoming.
			if !ok {
				if orphans, ok := orphanage[f.JobID]; ok {
					orphanage[f.JobID] = append(orphans, f)
				} else {
					orphanage[f.JobID] = []Flow{f}
				}

				log.WithFields(log.Fields{
					"job":  f.JobID,
					"node": s.NodeID,
				}).Info("putting in orphanage")
				continue
			}

			select {
			case cf.incoming <- f:
			case <-time.After(time.Second):
				log.WithFields(log.Fields{
					"job":  f.JobID,
					"node": s.NodeID,
				}).Info("receiving flow stuck")
			}
		case jid := <-done:
			delete(coflows, jid)
			log.WithFields(log.Fields{
				"job":  jid,
				"node": s.NodeID,
			}).Info("coflow done incoming")
		}
	}
}

func (s *Sincronia) newCoflows() {
	newOutgoingCfs := make(chan coflowSlice)
	go s.outgoing(newOutgoingCfs)
	newIncomingCfs := make(chan coflowSlice)
	go s.incoming(newIncomingCfs)
	for cf := range s.newCoflow {
		if len(cf.send) > 0 {
			select {
			case newOutgoingCfs <- cf:
			case <-time.After(time.Second):
				log.WithFields(log.Fields{
					"node":   s.NodeID,
					"job":    cf.jobID,
					"coflow": cf,
				}).Panic("new outgoing stuck")
			}
		}

		if len(cf.recv) > 0 {
			select {
			case newIncomingCfs <- cf:
			case <-time.After(time.Second):
				log.WithFields(log.Fields{
					"node": s.NodeID,
					"job":  cf.jobID,
				}).Panic("new incoming stuck")
			}
		} else {
			close(cf.incoming)
			close(cf.ret)
			cf.incoming = nil
			cf.ret = nil
		}
	}
}

// New returns a Sincronia agent connected to the central scheduler
// schedAddr: central scheduler address
// nodeID: the unique id of this node
// nodeMap: a mapping of unique node ids to network addresses (i.e. 128.30.2.121:17000)
func New(schedAddr string, nodeID uint32, nodes map[uint32]string) (*Sincronia, error) {
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

	s := &Sincronia{
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
func (s *Sincronia) Stop() {
	close(s.stop)
}

// RegCoflow called by application master to tell scheduler about all coflows.
// Only call this once per coflow.
// Coflow sizes need not be known in advance (set to 0)
func (s *Sincronia) RegCoflow(cfs []Coflow) {
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

// SendCoflow tells Sincronia to send the CoflowSlice.
// Only call this method once per coflow per node.
// When the central scheduler returns a coflow priority,
// the flow is automatically sent over the network.
//
// DO NOT start sending goroutines
//
// Returns a channel on which incoming flows are returned
func (s *Sincronia) SendCoflow(
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
			"job":       cf.jobID,
			"node":      s.NodeID,
			"toSend":    cf.send,
			"toSendLen": len(cf.send),
			"toRecv":    cf.recv,
			"toRecvLen": len(cf.recv),
		}).Info("coflow is ready")
		select {
		case s.newCoflow <- cf:
		case <-time.After(10 * time.Second):
			log.WithFields(log.Fields{
				"job":       cf.jobID,
				"node":      s.NodeID,
				"toSend":    cf.send,
				"toSendLen": len(cf.send),
				"toRecv":    cf.recv,
				"toRecvLen": len(cf.recv),
			}).Panic("timeout new coflow")
		}
	}()

	return cf.ret, nil
}
