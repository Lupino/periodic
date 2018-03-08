package periodic

import (
	"bytes"
	"encoding/binary"
	"github.com/Lupino/go-periodic/protocol"
	"github.com/Lupino/periodic/driver"
	"io"
	"log"
	"sync"
)

type worker struct {
	jobQueue map[int64]driver.Job
	conn     protocol.Conn
	sched    *Sched
	alive    bool
	funcs    []string
	locker   *sync.Mutex
}

func encodeJobHandle(id int64) []byte {
	h64 := make([]byte, 8)
	binary.BigEndian.PutUint64(h64, uint64(id))

	buf := bytes.NewBuffer(nil)
	buf.WriteByte(byte(8))
	buf.Write(h64)
	return buf.Bytes()
}

func decodeJobHandle(data []byte) int64 {
	h64 := data[1:9]
	return int64(binary.BigEndian.Uint64(h64))
}

func newWorker(sched *Sched, conn protocol.Conn) (w *worker) {
	w = new(worker)
	w.conn = conn
	w.jobQueue = make(map[int64]driver.Job)
	w.sched = sched
	w.funcs = make([]string, 0)
	w.alive = true
	w.locker = new(sync.Mutex)
	return
}

func (w *worker) IsAlive() bool {
	return w.alive
}

func (w *worker) handleJobAssign(msgID []byte, job driver.Job) (err error) {
	defer w.locker.Unlock()
	w.locker.Lock()
	w.jobQueue[job.ID] = job
	buf := bytes.NewBuffer(nil)
	buf.Write(msgID)
	buf.Write(protocol.JOBASSIGN.Bytes())
	buf.Write(encodeJobHandle(job.ID))
	buf.Write(job.Encode())
	err = w.conn.Send(buf.Bytes())
	return
}

func (w *worker) handleCanDo(Func string) error {
	for _, f := range w.funcs {
		if f == Func {
			return nil
		}
	}
	w.funcs = append(w.funcs, Func)
	w.sched.incrStatFunc(Func)
	return nil
}

func (w *worker) handleCanNoDo(Func string) error {
	var newFuncs = make([]string, 0)
	for _, f := range w.funcs {
		if f == Func {
			continue
		}
		newFuncs = append(newFuncs, f)
	}
	w.funcs = newFuncs
	return nil
}

func (w *worker) handleDone(jobID int64) (err error) {
	w.sched.done(jobID)
	defer w.locker.Unlock()
	w.locker.Lock()
	if _, ok := w.jobQueue[jobID]; ok {
		delete(w.jobQueue, jobID)
	}
	return nil
}

func (w *worker) handleFail(jobID int64) (err error) {
	w.sched.fail(jobID)
	defer w.locker.Unlock()
	w.locker.Lock()
	if _, ok := w.jobQueue[jobID]; ok {
		delete(w.jobQueue, jobID)
	}
	return nil
}

func (w *worker) handleCommand(msgID []byte, cmd protocol.Command) (err error) {
	buf := bytes.NewBuffer(nil)
	buf.Write(msgID)
	buf.Write(cmd.Bytes())
	err = w.conn.Send(buf.Bytes())
	return
}

func (w *worker) handleSchedLater(jobID, delay, counter int64) (err error) {
	w.sched.schedLater(jobID, delay, counter)
	defer w.locker.Unlock()
	w.locker.Lock()
	if _, ok := w.jobQueue[jobID]; ok {
		delete(w.jobQueue, jobID)
	}
	return nil
}

func (w *worker) handleGrabJob(msgID []byte) (err error) {
	item := grabItem{
		w:     w,
		msgID: msgID,
	}
	w.sched.grabQueue.push(item)
	w.sched.notifyJobTimer()
	return nil
}

func (w *worker) handle() {
	var payload []byte
	var err error
	var conn = w.conn
	var msgID []byte
	var cmd protocol.Command
	defer func() {
		if x := recover(); x != nil {
			log.Printf("[worker] painc: %v\n", x)
		}
	}()
	defer w.Close()
	for {
		payload, err = conn.Receive()
		if err != nil {
			if err != io.EOF {
				log.Printf("workerError: %s\n", err.Error())
			}
			break
		}

		msgID, cmd, payload = protocol.ParseCommand(payload)

		switch cmd {
		case protocol.GRABJOB:
			err = w.handleGrabJob(msgID)
			break
		case protocol.WORKDONE:
			jobID := decodeJobHandle(payload)
			err = w.handleDone(jobID)
			break
		case protocol.WORKFAIL:
			jobID := decodeJobHandle(payload)
			err = w.handleFail(jobID)
			break
		case protocol.SCHEDLATER:
			jh := payload[0:9]
			jobID := decodeJobHandle(jh)
			h64 := payload[9:17]
			delay := int64(binary.BigEndian.Uint64(h64))

			h16 := payload[17:19]
			counter := int64(binary.BigEndian.Uint16(h16))
			err = w.handleSchedLater(jobID, delay, counter)
			break
		case protocol.SLEEP:
			err = w.handleCommand(msgID, protocol.NOOP)
			break
		case protocol.PING:
			err = w.handleCommand(msgID, protocol.PONG)
			break
		case protocol.CANDO:
			err = w.handleCanDo(string(payload[1:]))
			break
		case protocol.CANTDO:
			err = w.handleCanNoDo(string(payload[1:]))
			break
		default:
			err = w.handleCommand(msgID, protocol.UNKNOWN)
			break
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("workerError: %s\n", err.Error())
			}
			break
		}

		if !w.alive {
			break
		}
	}
}

func (w *worker) Close() {
	defer w.sched.notifyJobTimer()
	defer w.conn.Close()
	w.sched.grabQueue.removeWorker(w)
	w.alive = false
	for k := range w.jobQueue {
		w.sched.fail(k)
	}
	w.jobQueue = nil
	for _, Func := range w.funcs {
		w.sched.decrStatFunc(Func)
	}
	w = nil
}
