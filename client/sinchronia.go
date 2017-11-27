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
	From uint32
	To   uint32
	Info Data
}

// Coflow is a full view of a coflow, for registration
type Coflow struct {
	JobID uint32
	Flows []Flow
}

// CoflowSlice is a client's view of a coflow
type coflowSlice struct {
	jobID  uint32
	nodeID uint32
	send   []sendFlow
	recv   []Data
	prio   uint8
	done   chan Flow // return received Data to SendCoflow caller over this channel
}

// Sinchronia manages interactions with the coflow scheduler
// A "Node" is something which sends or receives flows belonging to coflows.
// The application calling this library should give each Node a unique id.
type Sinchronia struct {
	NodeID      uint32
	schedClient scheduler.Scheduler
	ctx         context.Context
	nodeMap     map[uint32]string
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

	ctx := context.Background()
	rpcConn := rpc.NewConn(rpc.StreamTransport(conn))
	sch := scheduler.Scheduler{Client: rpcConn.Bootstrap(ctx)}

	s := &Sinchronia{
		NodeID:      nodeID,
		schedClient: sch,
		ctx:         ctx,
		nodeMap:     nodes,
	}

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
	)
}

// SendCoflow tells sinchronia to send the CoflowSlice.
// Only call this method once per coflow per node.
// When the central scheduler returns a coflow priority,
// the flow is automatically sent over the network.
//
// Returns a channel on which incoming flows are returned
func (s *Sinchronia) SendCoflow(jobID uint32, flows []Flow) (chan Flow, error) {
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
	)

	// make a channel for each flow
	// and have it wait to send
	sendFlows := make([]sendFlow, 0, len(flows))
	for _, fl := range flows {
		sf := sendFlow{
			f:       fl,
			sendNow: make(chan uint8),
		}

		sendFlows = append(sendFlows, sf)
		go sf.send(s.ctx, jobID, s.nodeMap)
	}

	cf := coflowSlice{
		jobID:  jobID,
		nodeID: s.NodeID,
		send:   sendFlows,
		recv:   []Data{}, // unused in send phase
		prio:   0,        // unused in send phase
		done:   make(chan Flow, 10),
	}

	go listen(jobID, s.NodeID, 17000, cf.done)
	go s.sendCoflow(cf)
	return cf.done, nil
}

func (s *Sinchronia) sendCoflow(cf coflowSlice) {
	// wait for coflow schedule on this node
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
		}).Error(err)
		return
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

	cf.prio = uint8(schedule.Priority())

	// TODO check this list of received against actually received
	//recvs, err := schedule.Receiving()
	//if err != nil {
	//	log.WithFields(log.Fields{
	//		"node_id": s.NodeID,
	//		"job_id":  cf.jobID,
	//		"where":   "getSchedule - read receiving",
	//	}).Error(err)
	//	return
	//}
	//
	//cf.recv = make([]Data, 0, recvs.Len())
	//for i, r := range recvs {
	//	f := Data{
	//		DataID: r.DataID(),
	//		Size:   r.Size(),
	//		Blob:   make([]byte, r.Size()),
	//	}
	//
	//	cf.recv = append(cf.recv, f)
	//}

	// and signal outgoing flows to send with appropriate priority
	for _, f := range cf.send {
		f.sendNow <- cf.prio
	}
}
