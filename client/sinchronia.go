package client

import (
	"context"
	"fmt"
	"net"
	"sort"
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
	send   []sendFlow
	recv   []Data

	sendNow chan uint32      // send one flow, with the given priority
	sent    chan interface{} // the one flow was sent

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
}

func (s *Sinchronia) getSchedule() ([]coflowScheduleItem, error) {
	scheduleRes, err := s.schedClient.GetSchedule(
		s.ctx,
		func(
			p scheduler.Scheduler_getSchedule_Params,
		) error {
			p.SetNodeId(s.NodeID)
			return nil
		},
	).Struct()

	if err != nil {
		return nil, err
	}

	schRet, err := scheduleRes.Schedule()
	if schRet.Len() == 0 {
		return nil, fmt.Errorf("no scheduled coflows")
	}

	sch := make([]coflowScheduleItem, 0, schRet.Len())
	for i := 0; i < schRet.Len(); i++ {
		sch = append(sch, coflowScheduleItem{
			jobID:    schRet.At(i).JobID(),
			priority: schRet.At(i).Priority(),
		})
	}

	sort.Slice(sch, func(i, j int) bool {
		return sch[i].priority < sch[j].priority
	})

	return sch, nil
}

func after(ch chan interface{}) {
	<-time.After(100 * time.Millisecond)
	ch <- struct{}{}
}

// Main control loop
// Sending Loop
// 1. Poll the master for the CoflowSchedule
// 2. Of all the local coflows, send the highest priority one
//
// Receiving
// Distribute incoming flows across all coflows to the correct coflowSlice
func (s *Sinchronia) start() {
	var ok bool
	var err error
	coflows := make(map[uint32]coflowSlice)
	var currSchedule []coflowScheduleItem

	// initialize currCf to a dummy that triggers after 100ms
	ch := make(chan interface{})
	currCf := coflowSlice{sent: ch}
	go after(ch)

	for {
		select {
		case f := <-s.recv:
			cf := coflows[f.JobID]
			go func(c coflowSlice) { c.incoming <- f }(cf)
			continue
		case cf := <-s.newCoflow:
			coflows[cf.jobID] = cf
			go s.sendCoflow(cf)
		case <-currCf.sent:
		}

		// 1. Poll the master for the CoflowSchedule
		currSchedule, err = s.getSchedule()
		if err != nil {
			log.WithFields(log.Fields{
				"nodeID": s.NodeID,
				"err":    err,
			}).Warn("GetSchedule RPC")
		}

		if len(currSchedule) == 0 {
			// no flows to send, so use a dummy coflowSlice for a timeout
			currCf = coflowSlice{sent: ch}
			go after(ch)
			continue
		}

		// 2. Of all the local coflows, send the highest priority one
		scheduled := currSchedule[0]
		currSchedule = currSchedule[1:]
		currCf, ok = coflows[scheduled.jobID]
		if !ok {
			// why does this coflow not exist !?
			log.WithFields(log.Fields{
				"nodeID":   s.NodeID,
				"jobID":    scheduled.jobID,
				"priority": scheduled.priority,
				"coflows":  coflows,
			}).Panic("scheduled coflow not found")
		}

		log.WithFields(log.Fields{
			"nodeID": s.NodeID,
			"currCf": currCf,
		}).Info("sending")
		currCf.sendNow <- scheduled.priority
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
	}

	go s.start()
	go listen(s.NodeID, s.nodeMap[s.NodeID], s.recv)

	return s, nil
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
	sendFlows := make([]sendFlow, 0, len(flows))
	for _, fl := range flows {
		// make a channel for each flow
		sf := sendFlow{
			f:    fl,
			done: make(chan interface{}),
		}

		sendFlows = append(sendFlows, sf)
	}

	cf := coflowSlice{
		jobID:    jobID,
		nodeID:   s.NodeID,
		send:     sendFlows,
		sendNow:  make(chan uint32),
		sent:     make(chan interface{}),
		incoming: make(chan Flow, 10),
		ret:      make(chan Data, 10),
	}

	s.newCoflow <- cf
	return cf.ret, nil
}
