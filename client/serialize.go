package client

import (
	"bytes"
	"encoding/json"
	"net"
)

// Data is the serialization type for a flow
type Data struct {
	DataID uint32
	Len    uint32
}

// SendCoflow is the serialization type for registering coflows
type SendCoflow struct {
	JobID uint32
	Flows []Data
}

func serialize(conn *net.Conn, cfs chan Coflow) {
	enc := json.NewEncoder(conn)
	for cf := range cfs {
		sendFlows := make([]SendData, 0, len(cf.send))
		for f := range cf.send {
			sendFlows = append(sendFlows, Data{f.DataID, len(f.Data)})
		}

		scf := SendCoflow{JobID: cf.JobID, Flows: sendFlows}
		if err := enc.Encode(&scf); err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Warn("send/encode")
		}
	}
}

// RecvCoflow specifies the priority of a coflow, and what data it should receive
type RecvCoflow struct {
	JobID     uint32
	Priority  uint8
	Receiving []Data
}

func deserialize(conn *net.Conn, cfs chan Coflow) {
	dec := json.NewDecoder(conn)
	var rcfs []RecvCoflow
	for {
		if err := dec.Decode(&rcfs); err != nil {
			log.WithFields(log.Fields{
				"err": err,
			}).Warn("recv/decode")
		}

		// for each coflow for which the priority and receivers were specified
		for rcf := range rcfs {
			rcvf := make([]RecvFlow, 0, len(rcf.Receiving))
			for rd := range rcf.Receiving {
				rcvf = append(rcvf, RecvFlow{
					JobID:  rcf.JobID,
					DataID: rd.DataID,
					Data:   make([]uint8, 0, rd.Len),
				})
			}

			cfs <- Coflow{
				jobID: rcf.JobID,
				prio:  rcf.Priority,
				recv:  rcvf,
			}
		}
	}
}
