package client

import (
	"net"
)

func getPortForDataID(data uint32) uint16 {
	return uint16(data) + BaseRecvPort
}

// RecvFlow is an incoming flow
type RecvFlow struct {
	JobID  uint32  // which coflow
	DataID uint32  // which piece of data this is (flow id)
	Data   []uint8 // received data
}

func (f *RecvFlow) recv(done chan RecvFlow) {
	port := getPortForDataID(f.DataID)
	ln, err := net.Listen("tcp4", port)
	if err != nil {
		log.WithFields(log.Fields{
			"port":   port,
			"jobId":  f.JobID,
			"dataId": f.DataID,
		}).Warn("listen")
		return
	}

	var conn *net.Conn
	for {
		conn, err = ln.Accept()
		if err != nil {
			log.WithFields(log.Fields{
				"port":   port,
				"jobId":  f.JobID,
				"dataId": f.DataID,
			}).Warn("open")
			return
		}

		if conn.RemoteAddr() == f.From {
			break
		}
	}

	read, err := conn.Read(f.Data)
	if err != nil {
		log.WithFields(log.Fields{
			"port":   port,
			"jobId":  f.JobID,
			"dataId": f.DataID,
		}).Warn("read")
		return
	}

	log.WithFields(log.Fields{
		"port":   port,
		"jobId":  f.JobID,
		"dataId": f.DataID,
		"read":   read,
	}).Info("read")
	done <- f
}
