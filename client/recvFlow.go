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
			"jobid":        params.JobID(),
			"where":        "send - params",
		}).Warn(err)
	}

	if params.To() != r.nodeID {
		log.WithFields(log.Fields{
			"node_id":      r.nodeID,
			"given_nodeid": params.To(),
			"jobid":        params.JobID(),
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
			"nodeId": r.nodeID,
			"dataId": params.DataID,
			"read":   len(blob),
			"where":  "send - read",
		}).Error(err)
		return err
	}

	log.WithFields(log.Fields{
		"nodeId": r.nodeID,
		"jobid":  params.JobID(),
		"dataId": params.DataID,
		"read":   len(blob),
	}).Info("read")

	r.done <- Flow{
		JobID: params.JobID(),
		From:  params.From(),
		To:    params.To(),
		Info: Data{
			DataID: params.DataID(),
			Size:   uint32(len(blob)),
			Blob:   blob,
		},
	}

	return nil
}

func listen(
	node uint32,
	address string,
	done chan Flow,
) {
	ln, err := net.Listen("tcp4", address)
	if err != nil {
		log.WithFields(log.Fields{
			"nodeId":  node,
			"address": address,
			"where":   "listen",
		}).Error(err)
		return
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.WithFields(log.Fields{
				"nodeId":  node,
				"address": address,
				"where":   "open",
			}).Error(err)
			return
		}

		impl := scheduler.Receiver_ServerToClient(Recv{
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
					"nodeId":  node,
					"address": address,
					"where":   "rpc server wait",
				}).Error(err)
			}
		}(rpcConn)
	}
}
