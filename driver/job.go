package driver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Lupino/go-periodic/protocol"
	"strconv"
)

// Job workload.
type Job struct {
	ID      int64  `json:"job_id"`
	Name    string `json:"name"`     // The job name, this is unique.
	Func    string `json:"func"`     // The job function reffer on worker function
	Args    string `json:"workload"` // Job args
	Timeout int64  `json:"timeout"`  // Job processing timeout
	SchedAt int64  `json:"sched_at"` // When to sched the job.
	RunAt   int64  `json:"run_at"`   // The job is start at
	Counter int64  `json:"counter"`  // The job run counter
	Status  string `json:"status"`
}

// IsReady check job status ready
func (job Job) IsReady() bool {
	return job.Status == "ready"
}

// IsProc check job status processing
func (job Job) IsProc() bool {
	return job.Status == "processing"
}

// SetReady set job status ready
func (job *Job) SetReady() {
	job.Status = "ready"
}

// SetProc set job status processing
func (job *Job) SetProc() {
	job.Status = "processing"
}

// NewJob create a job from json bytes
func NewJob(payload []byte) (job Job, err error) {
	err = json.Unmarshal(payload, &job)
	return
}

// Bytes encode job to json bytes
func (job Job) Bytes() (data []byte) {
	data, _ = json.Marshal(job)
	return
}

// Decode create a job from bytes
func Decode(payload []byte) (job Job, err error) {
	parts := bytes.SplitN(payload, protocol.NullChar, 5)
	partSize := len(parts)
	if partSize < 2 {
		err = fmt.Errorf("InvalID %v\n", payload)
		return
	}
	job.Func = string(parts[0])
	job.Name = string(parts[1])
	if partSize > 2 {
		job.SchedAt, _ = strconv.ParseInt(string(parts[2]), 10, 0)
	}
	if partSize > 3 {
		job.Counter, _ = strconv.ParseInt(string(parts[3]), 10, 0)
	}
	if partSize > 4 {
		job.Args = string(parts[4])
	}
	return
}

// Encode job to bytes
func (job Job) Encode() []byte {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(job.Func)
	buf.Write(protocol.NullChar)
	buf.WriteString(job.Name)
	argc := len(job.Args)
	if argc > 0 || job.SchedAt > 0 || job.Counter > 0 {
		buf.Write(protocol.NullChar)
		buf.WriteString(strconv.FormatInt(job.SchedAt, 10))
	}
	if argc > 0 || job.Counter > 0 {
		buf.Write(protocol.NullChar)
		buf.WriteString(strconv.FormatInt(job.Counter, 10))
	}
	if argc > 0 {
		buf.Write(protocol.NullChar)
		buf.WriteString(job.Args)
	}
	return buf.Bytes()
}
