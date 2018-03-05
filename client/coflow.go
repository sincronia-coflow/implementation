package client

import (
	"sort"
	"time"

	"github.com/akshayknarayan/sincronia/client/scheduler"

	log "github.com/sirupsen/logrus"
)

func (s *Sincronia) getSchedule() ([]coflowScheduleItem, error) {
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
		return []coflowScheduleItem{}, nil
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

func (s *Sincronia) coflowReady(cf coflowSlice) (toRecv map[uint32]Data) {
rpcReq:
	recvsRet, err := s.schedClient.SendCoflow(
		s.ctx,
		func(
			cs scheduler.Scheduler_sendCoflow_Params,
		) error {
			cs.SetJobID(cf.jobID)
			cs.SetNodeID(s.NodeID)
			sfs, err := cs.NewSending(int32(len(cf.send)))
			if err != nil {
				log.WithFields(log.Fields{
					"jobId": cf.jobID,
					"where": "sendCoflow",
				}).Error(err)
				return err
			}

			i := 0
			for _, sf := range cf.send {
				d := sfs.At(i)
				i++
				d.SetDataID(sf.f.Info.DataID)
				d.SetSize(sf.f.Info.Size)
			}

			return nil
		},
	).Struct()
	if err != nil {
		log.WithFields(log.Fields{
			"node": s.NodeID,
			"err":  err,
		}).Warn("SendCoflow() RPC failed")
		goto rpcReq // retry the rpc request until it succeeds.
	}

	recvs, err := recvsRet.Receiving()
	if err != nil {
		panic(err)
	}

	toRecv = make(map[uint32]Data)
	for i := 0; i < recvs.Len(); i++ {
		r := recvs.At(i)
		d := Data{
			DataID: r.DataID(),
			Size:   r.Size(),
		}

		toRecv[d.DataID] = d
	}

	return
}

func (cf *coflowSlice) sendOneFlow(s *Sincronia, prio uint32) {
	if len(cf.send) == 0 {
		return
	}

	// pick an arbtrary flow
	for dataid, sf := range cf.send {
		log.WithFields(log.Fields{
			"node":      s.NodeID,
			"coflow":    cf.jobID,
			"flow":      sf,
			"remaining": cf.send,
		}).Info("sending flow")
		delete(cf.send, dataid)
		go sf.send(s.ctx, cf.jobID, prio, s.nodeMap)
		select {
		case <-sf.done:
		case <-time.After(10 * time.Second):
			log.WithFields(log.Fields{
				"node":      s.NodeID,
				"coflow":    cf.jobID,
				"flow":      sf,
				"remaining": cf.send,
			}).Panic("timed out")
		}
		return
	}
}

func (s *Sincronia) recvExpected(cf coflowSlice, done chan uint32) {
	// receive expected flows
	node := s.NodeID
	if len(cf.recv) == 0 {
		// no data to receive
		close(cf.ret)
		return
	}

	recvsCopy := make(map[uint32]Data)
	for k, v := range cf.recv {
		recvsCopy[k] = v
	}

	log.WithFields(log.Fields{
		"node":    node,
		"job":     cf.jobID,
		"where":   "recvExpected",
		"cf.recv": cf.recv,
	}).Info("expecting incoming data")

	for f := range cf.incoming {
		if d, ok := cf.recv[f.Info.DataID]; ok {
			d.Blob = f.Info.Blob[:]
			go func(d Data) { cf.ret <- d }(d) // don't block on the application
			log.WithFields(log.Fields{
				"node":  node,
				"job":   cf.jobID,
				"data":  d.DataID,
				"from":  f.From,
				"where": "recvExpected",
			}).Info("returned received data to caller")
			delete(cf.recv, d.DataID)
			if len(cf.recv) == 0 {
				break
			}
		} else if _, ok := recvsCopy[f.Info.DataID]; ok {
			log.WithFields(log.Fields{
				"node":  node,
				"job":   cf.jobID,
				"data":  d.DataID,
				"from":  f.From,
				"where": "recvExpected",
			}).Warn("received flow duplicate")
		} else {
			log.WithFields(log.Fields{
				"node":  node,
				"job":   cf.jobID,
				"data":  d.DataID,
				"from":  f.From,
				"where": "recvExpected",
			}).Warn("received unexpected flow")
		}
	}

	log.WithFields(log.Fields{
		"node":  node,
		"job":   cf.jobID,
		"recvd": recvsCopy,
	}).Info("coflow slice done receiving")
reportDone:
	_, err := s.schedClient.CoflowDone(
		s.ctx,
		func(
			p scheduler.Scheduler_coflowDone_Params,
		) error {
			p.SetJobId(cf.jobID)
			p.SetNodeId(node)

			fin, err := p.NewFinished(int32(len(recvsCopy)))
			if err != nil {
				panic(err)
				//return err
			}

			i := 0
			for d := range recvsCopy {
				fin.Set(i, d)
				i++
			}

			return nil
		},
	).Struct()

	if err != nil {
		log.WithFields(log.Fields{
			"node":  node,
			"job":   cf.jobID,
			"where": "CoflowDone()",
			"err":   err,
		}).Warn("CoflowDone() error")
		goto reportDone
	}

	close(cf.ret)
	done <- cf.jobID
}
