package periodic

import (
	"bytes"
	"github.com/Lupino/go-periodic/protocol"
	"github.com/Lupino/periodic/driver"
	"io"
	"log"
)

type client struct {
	sched *Sched
	conn  protocol.Conn
}

func newClient(sched *Sched, conn protocol.Conn) (c *client) {
	c = new(client)
	c.conn = conn
	c.sched = sched
	return
}

func (c *client) handle() {
	var payload []byte
	var err error
	var msgID []byte
	var cmd protocol.Command
	var conn = c.conn
	defer func() {
		if x := recover(); x != nil {
			log.Printf("[client] painc: %v\n", x)
		}
	}()
	defer conn.Close()
	for {
		payload, err = conn.Receive()
		if err != nil {
			if err != io.EOF {
				log.Printf("clientError: %s\n", err.Error())
			}
			return
		}

		msgID, cmd, payload = protocol.ParseCommand(payload)

		switch cmd {
		case protocol.SUBMITJOB:
			err = c.handleSubmitJob(msgID, payload)
			break
		case protocol.STATUS:
			err = c.handleStatus(msgID)
			break
		case protocol.PING:
			err = c.handleCommand(msgID, protocol.PONG)
			break
		case protocol.DROPFUNC:
			err = c.handleDropFunc(msgID, payload)
			break
		case protocol.REMOVEJOB:
			err = c.handleRemoveJob(msgID, payload)
			break
		default:
			err = c.handleCommand(msgID, protocol.UNKNOWN)
			break
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("clientError: %s\n", err.Error())
			}
			return
		}
	}
}

func (c *client) handleCommand(msgID []byte, cmd protocol.Command) (err error) {
	buf := bytes.NewBuffer(nil)
	buf.Write(msgID)
	buf.Write(cmd.Bytes())
	err = c.conn.Send(buf.Bytes())
	return
}

func (c *client) handleSubmitJob(msgID []byte, payload []byte) (err error) {
	var job driver.Job
	var e error
	var conn = c.conn
	var sched = c.sched
	defer sched.jobLocker.Unlock()
	sched.jobLocker.Lock()
	job, e = driver.Decode(payload)
	if e != nil {
		err = conn.Send([]byte(e.Error()))
		return
	}
	isNew := true
	changed := false
	job.SetReady()
	oldJob, e := sched.driver.GetOne(job.Func, job.Name)
	if e == nil && oldJob.ID > 0 {
		job.ID = oldJob.ID
		if oldJob.IsProc() {
			sched.decrStatProc(oldJob)
			sched.removeRevertPQ(job)
			changed = true
		}
		isNew = false
	}
	e = sched.driver.Save(&job)
	if e != nil {
		err = conn.Send([]byte(e.Error()))
		return
	}

	if isNew {
		sched.incrStatJob(job)
	}
	if isNew || changed {
		sched.pushJobPQ(job)
	}
	sched.notifyJobTimer()
	err = c.handleCommand(msgID, protocol.SUCCESS)
	return
}

func (c *client) handleStatus(msgID []byte) (err error) {
	buf := bytes.NewBuffer(nil)
	buf.Write(msgID)
	defer c.sched.funcLocker.Unlock()
	c.sched.funcLocker.Lock()
	for _, stat := range c.sched.stats {
		buf.WriteString(stat.String())
		buf.WriteString("\n")
	}
	err = c.conn.Send(buf.Bytes())
	return
}

func (c *client) handleDropFunc(msgID []byte, payload []byte) (err error) {
	Func := string(payload)
	sched := c.sched
	defer sched.notifyJobTimer()
	defer sched.jobLocker.Unlock()
	sched.jobLocker.Lock()

	defer sched.funcLocker.Unlock()
	sched.funcLocker.Lock()
	stat, ok := sched.stats[Func]
	if ok && stat.Worker.Int() == 0 {
		iter := sched.driver.NewIterator(payload)
		var deleteJob = make([]int64, 0)
		for {
			if !iter.Next() {
				break
			}
			job := iter.Value()
			deleteJob = append(deleteJob, job.ID)
		}
		iter.Close()
		for _, jobID := range deleteJob {
			sched.driver.Delete(jobID)
		}
		delete(sched.stats, Func)
		delete(sched.jobPQ, Func)
	}
	err = c.handleCommand(msgID, protocol.SUCCESS)
	return
}

func (c *client) handleRemoveJob(msgID, payload []byte) (err error) {
	var job driver.Job
	var e error
	var conn = c.conn
	var sched = c.sched
	defer sched.jobLocker.Unlock()
	sched.jobLocker.Lock()
	job, e = driver.Decode(payload)
	if e != nil {
		err = conn.Send([]byte(e.Error()))
		return
	}
	job, e = sched.driver.GetOne(job.Func, job.Name)
	if e == nil && job.ID > 0 {
		if _, ok := sched.procQueue[job.ID]; ok {
			delete(sched.procQueue, job.ID)
		}
		sched.driver.Delete(job.ID)
		sched.decrStatJob(job)
		if job.IsProc() {
			sched.decrStatProc(job)
			sched.removeRevertPQ(job)
		}
		sched.notifyJobTimer()
	}

	if e != nil {
		err = conn.Send([]byte(e.Error()))
	} else {
		err = c.handleCommand(msgID, protocol.SUCCESS)
	}
	return
}
