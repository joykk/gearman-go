package worker

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"sync"
	"time"
)

// The agent of job server.
type agent struct {
	sync.Mutex
	conn         net.Conn
	rw           *bufio.ReadWriter
	worker       *Worker
	in           chan []byte
	net, addr    string
	ErrorHandler ErrorHandler
}

// Create the agent of job server.
func newAgent(net, addr string, worker *Worker) (a *agent, err error) {
	a = &agent{
		net:    net,
		addr:   addr,
		worker: worker,
		in:     make(chan []byte, queueSize),
	}
	return
}

func (a *agent) Connect() (err error) {
	a.Lock()
	defer a.Unlock()
	a.conn, err = net.DialTimeout(a.net, a.addr, a.worker.agentConnectTimeOut)
	if err != nil {
		return
	}
	a.rw = bufio.NewReadWriter(bufio.NewReader(a.conn),
		bufio.NewWriter(a.conn))
	go a.work()
	return
}

func (a *agent) work() {
	defer func() {
		if err := recover(); err != nil {
			a.worker.err(err.(error))
		}
	}()

	var inpack *inPack
	var l int
	var err error
	var data, leftdata []byte
	for {
		if data, err = a.read(); err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Temporary() {
					continue
				} else {
					logrus.WithError(err).WithField("addr", a.addr).Debug("a.disconnect_error(err)1")
					if a.worker.agentAutoReconnect {
						time.Sleep(a.worker.agentAutoReconnectWaitTime)
						err = a.reconnect()
						if err != nil {
							logrus.WithError(err).WithField("agentlen", len(a.worker.agents)).Error("reconnect err")
						} else {
							logrus.WithError(err).WithField("agentlen", len(a.worker.agents)).Info("reconnect ok")
						}
						return
					} else {
						// else - we're probably dc'ing due to a Close()
						a.disconnect_error(err)
						break
					}
				}
			} else if err == io.EOF {
				logrus.WithError(err).WithField("addr", a.addr).Debug("a.disconnect_error(err)2")
				if a.worker.agentAutoReconnect {
					time.Sleep(a.worker.agentAutoReconnectWaitTime)
					err = a.reconnect()
					if err != nil {
						logrus.WithError(err).WithField("agentlen", len(a.worker.agents)).Error("reconnect err")
					} else {
						logrus.WithError(err).WithField("agentlen", len(a.worker.agents)).Info("reconnect ok")
					}
					return
				} else {
					a.disconnect_error(err)
					break
				}
			}
			a.worker.err(err)
			// If it is unexpected error and the connection wasn't
			// closed by Gearmand, the agent should close the conection
			// and reconnect to job server.
			a.Close()
			a.conn, err = net.DialTimeout(a.net, a.addr, a.worker.agentConnectTimeOut)
			if err != nil {
				a.worker.err(err)
				break
			}
			a.rw = bufio.NewReadWriter(bufio.NewReader(a.conn),
				bufio.NewWriter(a.conn))
		}
		if len(leftdata) > 0 { // some data left for processing
			data = append(leftdata, data...)
		}
		if len(data) < minPacketLength { // not enough data
			leftdata = data
			continue
		}
		for {
			if inpack, l, err = decodeInPack(data); err != nil {
				a.worker.err(err)
				leftdata = data
				break
			} else {
				leftdata = nil
				inpack.a = a
				a.worker.in <- inpack
				if len(data) == l {
					break
				}
				if len(data) > l {
					data = data[l:]
				}
			}
		}
	}
	logrus.Warn("Start del agent")
	a.Lock()
	defer a.Unlock()
	for i, _ := range a.worker.agents {
		if a.worker.agents[i] == a {
			a.worker.agents = append(a.worker.agents[:i], a.worker.agents[i+1:]...)
			logrus.Warn("End del agent")
			return
		}
	}
}

func (a *agent) disconnect_error(err error) {
	a.Lock()
	defer a.Unlock()
	if a.conn != nil {
		err = &WorkerDisconnectError{
			err:   err,
			agent: a,
		}
		a.worker.err(err)
	}
}

func (a *agent) Close() {
	a.Lock()
	defer a.Unlock()
	if a.conn != nil {
		a.conn.Close()
		a.conn = nil
	}
}

func (a *agent) Grab() {
	a.Lock()
	defer a.Unlock()
	a.grab()
}

func (a *agent) grab() {
	outpack := getOutPack()
	outpack.dataType = dtGrabJobUniq
	a.write(outpack)
}

func (a *agent) PreSleep() {
	a.Lock()
	defer a.Unlock()
	outpack := getOutPack()
	outpack.dataType = dtPreSleep
	a.write(outpack)
}

func (a *agent) reconnect() error {
	a.Lock()
	defer a.Unlock()
	conn, err := net.DialTimeout(a.net, a.addr, a.worker.agentConnectTimeOut)
	if err != nil {
		time.Sleep(a.worker.agentAutoReconnectWaitTime)
		logrus.WithField("func", "reconnect").WithField("DialTimeout", err).Error("err")
		return a.reconnect()
	}
	a.conn = conn
	a.rw = bufio.NewReadWriter(bufio.NewReader(a.conn),
		bufio.NewWriter(a.conn))

	a.worker.reRegisterFuncsForAgent(a)
	a.grab()

	go a.work()
	return nil
}

// read length bytes from the socket
func (a *agent) read() (data []byte, err error) {
	n := 0

	tmp := getBuffer(bufferSize)
	var buf bytes.Buffer

	// read the header so we can get the length of the data
	if n, err = a.rw.Read(tmp); err != nil {
		return
	}
	dl := int(binary.BigEndian.Uint32(tmp[8:12]))

	// write what we read so far
	buf.Write(tmp[:n])

	// read until we receive all the data
	for buf.Len() < dl+minPacketLength {
		if n, err = a.rw.Read(tmp); err != nil {
			return buf.Bytes(), err
		}

		buf.Write(tmp[:n])
	}

	return buf.Bytes(), err
}

// Internal write the encoded job.
func (a *agent) write(outpack *outPack) (err error) {
	var n int
	buf := outpack.Encode()
	for i := 0; i < len(buf); i += n {
		n, err = a.rw.Write(buf[i:])
		if err != nil {
			return err
		}
	}
	return a.rw.Flush()
}

// Write with lock
func (a *agent) Write(outpack *outPack) (err error) {
	a.Lock()
	defer a.Unlock()
	return a.write(outpack)
}
