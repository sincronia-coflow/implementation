package client

import (
	"./scheduler"

	log "github.com/sirupsen/logrus"
)

// sendCoflow manages sending a single coflow
func (s *Sinchronia) sendCoflow(cf coflowSlice) {
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

			for i, sf := range cf.send {
				d := sfs.At(i)
				d.SetDataID(sf.f.Info.DataID)
				d.SetSize(sf.f.Info.Size)
			}

			return nil
		},
	).Struct()
	if err != nil {
		log.WithFields(log.Fields{
			"nodeID": s.NodeID,
			"err":    err,
		}).Warn("SendCoflow() RPC failed")
		goto rpcReq // retry the rpc request until it succeeds.
	}

	recvs, err := recvsRet.Receiving()
	if err != nil {
		panic(err)
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

	go s.recvExpected(cf, toRecv)

	// loop through sending each flow
	for _, sf := range cf.send {
		prio := <-cf.sendNow
		log.WithFields(log.Fields{
			"nodeID": s.NodeID,
			"coflow": cf.jobID,
			"flow":   sf,
		}).Info("sending flow")
		go sf.send(s.ctx, cf.jobID, prio, s.nodeMap)
		<-sf.done
		cf.sent <- struct{}{}
	}
}

func (s *Sinchronia) recvExpected(cf coflowSlice, recv []Data) {
	// receive expected flows
	node := s.NodeID
	if len(recv) == 0 {
		// no data to receive
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
			}).Info("returned received data to caller")
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

	log.WithFields(log.Fields{
		"node_id": node,
		"job_id":  cf.jobID,
		"recvd":   recv,
	}).Info("coflow slice done receiving")
	s.schedClient.CoflowDone(
		s.ctx,
		func(
			p scheduler.Scheduler_coflowDone_Params,
		) error {
			p.SetJobId(cf.jobID)
			p.SetNodeId(node)

			fin, err := p.NewFinished(int32(len(recv)))
			if err != nil {
				return err
			}

			for i, d := range recv {
				fin.Set(i, d.DataID)
			}

			return nil
		},
	).Struct()

	close(cf.ret)
}
