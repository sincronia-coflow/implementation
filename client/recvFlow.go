package client

import (
	"fmt"
	"net"

	"./scheduler"

	log "github.com/sirupsen/logrus"
	"zombiezen.com/go/capnproto2/rpc"
)

// Recv implements the Receiver RPC interface
type Recv struct {
	jobID  uint32
	nodeID uint32
	done   chan Flow
}

// Send is the RPC endpoint which receives data
func (r Recv) Send(call scheduler.Receiver_send) error {
	params, err := call.Params.Data()
	if err != nil {
		log.WithFields(log.Fields{
			"node_id":      r.nodeID,
			"given_nodeid": params.To(),
			"job_id":       r.jobID,
			"given_jobid":  params.JobID(),
			"where":        "send - params",
		}).Warn(err)
	}

	if params.JobID() != r.jobID {
		log.WithFields(log.Fields{
			"node_id":      r.nodeID,
			"given_nodeid": params.To(),
			"job_id":       r.jobID,
			"given_jobid":  params.JobID(),
			"where":        "send - jobId",
		}).Warn("received job id does not match")
		return fmt.Errorf(
			"received job id does not match: %d != %d",
			params.JobID(),
			r.jobID,
		)
	} else if params.To() != r.nodeID {
		log.WithFields(log.Fields{
			"node_id":      r.nodeID,
			"given_nodeid": params.To(),
			"job_id":       r.jobID,
			"given_jobid":  params.JobID(),
			"where":        "send - nodeId",
		}).Warn("received destination node id does not match")
		return fmt.Errorf(
			"received destination node id does not match: %d != %d",
			params.To(),
			r.nodeID,
		)
	}

	blob, err := params.Blob()
	if err != nil {
		log.WithFields(log.Fields{
			"jobId":  r.jobID,
			"nodeId": r.nodeID,
			"dataId": params.DataID,
			"read":   len(blob),
			"where":  "send - read",
		}).Error(err)
		return err
	}

	log.WithFields(log.Fields{
		"jobId":  r.jobID,
		"nodeId": r.nodeID,
		"dataId": params.DataID,
		"read":   len(blob),
	}).Info("read")

	r.done <- Flow{
		From: params.From(),
		To:   params.To(),
		Info: Data{
			DataID: params.DataID(),
			Size:   uint32(len(blob)),
			Blob:   blob,
		},
	}

	return nil
}

func listen(job uint32, node uint32, port uint16, done chan Flow) {
	ln, err := net.Listen("tcp4", fmt.Sprintf(":%d", port))
	if err != nil {
		log.WithFields(log.Fields{
			"port":   port,
			"jobId":  job,
			"nodeId": node,
			"where":  "listen",
		}).Error(err)
		return
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.WithFields(log.Fields{
				"port":   port,
				"jobId":  job,
				"nodeId": node,
				"where":  "open",
			}).Error(err)
			return
		}

		impl := scheduler.Receiver_ServerToClient(Recv{
			jobID:  job,
			nodeID: node,
			done:   done,
		})

		rpcConn := rpc.NewConn(
			rpc.StreamTransport(conn),
			rpc.MainInterface(impl.Client),
		)

		go func(c *rpc.Conn) {
			err := c.Wait()
			if err != nil {
				log.WithFields(log.Fields{
					"port":   port,
					"jobId":  job,
					"nodeId": node,
					"where":  "rpc server wait",
				}).Error(err)
			}
		}(rpcConn)
	}
}
