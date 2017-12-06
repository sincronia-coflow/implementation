package client

import (
	"context"
	"fmt"
	"net"

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

// CoflowSlice is a client's view of a coflow
type coflowSlice struct {
	jobID    uint32
	nodeID   uint32
	send     []sendFlow
	recv     []Data
	prio     uint8
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

// distribute incoming flows across all coflows to the correct coflowSlice
func getIncoming(newCoflow chan coflowSlice, recv chan Flow) {
	coflows := make(map[uint32]coflowSlice)
	for {
		select {
		case cf := <-newCoflow:
			coflows[cf.jobID] = cf
		case f := <-recv:
			cf := coflows[f.JobID]
			go func(c coflowSlice) { c.incoming <- f }(cf)
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
	}

	go getIncoming(s.newCoflow, s.recv)
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
// Returns a channel on which incoming flows are returned
func (s *Sinchronia) SendCoflow(
	jobID uint32,
	flows []Flow,
) (chan Data, error) {
	sendFlows := make([]sendFlow, 0, len(flows))
	if len(flows) > 0 {
		s.schedClient.SendCoflow(
			s.ctx,
			func(
				p scheduler.Scheduler_sendCoflow_Params,
			) error {
				cs, err := p.NewCoflowSlice()
				if err != nil {
					log.WithFields(log.Fields{
						"jobId": jobID,
						"where": "sendCoflow",
					}).Error(err)
					return err
				}

				cs.SetJobID(jobID)
				cs.SetNodeID(s.NodeID)
				sfs, err := cs.NewSending(int32(len(flows)))
				if err != nil {
					log.WithFields(log.Fields{
						"jobId": jobID,
						"where": "sendCoflow",
					}).Error(err)
					return err
				}

				for i, f := range flows {
					d := sfs.At(i)
					d.SetDataID(f.Info.DataID)
					d.SetSize(f.Info.Size)
				}
				return nil
			},
		).Struct()

		for _, fl := range flows {
			// make a channel for each flow
			sf := sendFlow{
				f:       fl,
				sendNow: make(chan uint8),
				done:    make(chan interface{}),
			}

			// and have it wait to send
			sendFlows = append(sendFlows, sf)
			go sf.send(s.ctx, jobID, s.nodeMap)
		}
	}

	cf := coflowSlice{
		jobID:    jobID,
		nodeID:   s.NodeID,
		send:     sendFlows,
		incoming: make(chan Flow, 10),
		ret:      make(chan Data, 10),
	}

	s.newCoflow <- cf
	go s.sendCoflow(cf)

	return cf.ret, nil
}

func (s *Sinchronia) sendCoflow(cf coflowSlice) {
	// wait for coflow schedule on this node
getSched:
	log.WithFields(log.Fields{
		"node": s.NodeID,
		"job":  cf.jobID,
	}).Info("calling GetSchedule")
	scheduleRes, err := s.schedClient.GetSchedule(
		s.ctx,
		func(
			p scheduler.Scheduler_getSchedule_Params,
		) error {
			p.SetJobId(cf.jobID)
			p.SetNodeId(s.NodeID)
			return nil
		},
	).Schedule().Struct()

	if err != nil {
		log.WithFields(log.Fields{
			"node_id": s.NodeID,
			"job_id":  cf.jobID,
			"where":   "getSchedule - rpc",
		}).Warn(err)
		goto getSched
	}

	if res := scheduleRes.Which(); res == scheduler.Scheduler_CoflowSchedule_Which_noSchedule {
		log.WithFields(log.Fields{
			"node_id": s.NodeID,
			"job_id":  cf.jobID,
			"where":   "getSchedule - parse",
		}).Warn("no schedule returned")
		return
	}

	schedule, err := scheduleRes.Schedule()
	if err != nil {
		log.WithFields(log.Fields{
			"node_id": s.NodeID,
			"job_id":  cf.jobID,
			"where":   "getSchedule - read schedule",
		}).Error(err)
		return
	}

	if schedule.JobID() != cf.jobID {
		log.WithFields(log.Fields{
			"node_id":        s.NodeID,
			"job_id":         cf.jobID,
			"returned_jobid": schedule.JobID(),
			"where":          "getSchedule - jobId",
		}).Warn("returned job id does not match")
		return
	}

	// expect the receipt of the appropriate data
	recvs, err := schedule.Receiving()
	if err != nil {
		log.WithFields(log.Fields{
			"node_id": s.NodeID,
			"job_id":  cf.jobID,
			"where":   "getSchedule - read receiving",
		}).Error(err)
		return
	}

	toRecv := make([]Data, 0, recvs.Len())
	for i := 0; i < recvs.Len(); i++ {
		r := recvs.At(i)
		d := Data{
			DataID: r.DataID(),
			Size:   r.Size(),
		}

		toRecv = append(toRecv, d)
	}

	go cf.recvExpected(s.NodeID, toRecv)

	// and signal outgoing flows to send with appropriate priority
	for _, f := range cf.send {
		f.sendNow <- uint8(schedule.Priority())
	}

	for _, f := range cf.send {
		<-f.done
	}
}

func (cf coflowSlice) recvExpected(node uint32, recv []Data) {
	if len(recv) == 0 {
		// no data to receive
		log.WithFields(log.Fields{
			"node_id": node,
			"job_id":  cf.jobID,
			"where":   "recvExpected",
		}).Info("no expected incoming data")
		close(cf.ret)
		return
	}

	toRecv := make(map[uint32]Data) // dataID -> Data
	for _, d := range recv {
		toRecv[d.DataID] = d
	}

	log.WithFields(log.Fields{
		"node_id": node,
		"job_id":  cf.jobID,
		"where":   "recvExpected",
		"toRecv":  toRecv,
	}).Info("expecting incoming data")

	for f := range cf.incoming {
		if d, ok := toRecv[f.Info.DataID]; ok {
			d.Blob = f.Info.Blob[:]
			cf.ret <- d
			log.WithFields(log.Fields{
				"node_id": node,
				"job_id":  cf.jobID,
				"data_id": d.DataID,
				"from":    f.From,
				"where":   "recvExpected",
			}).Info("returning received data to caller")
			delete(toRecv, d.DataID)
			if len(toRecv) == 0 {
				break
			}
		} else {
			log.WithFields(log.Fields{
				"node_id": node,
				"job_id":  cf.jobID,
				"data_id": d.DataID,
				"from":    f.From,
				"where":   "recvExpected",
			}).Warn("received unexpected flow")
		}
	}

	close(cf.ret)
}
