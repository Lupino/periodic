package driver

import (
	"encoding/json"
	"github.com/Lupino/go-periodic/types"
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
	j, err := types.NewJob(payload)
	if err != nil {
		return
	}
	job.Func = j.Func
	job.Name = j.Name
	job.Args = j.Args
	job.SchedAt = j.SchedAt
	job.Counter = j.Counter
	return
}

// Encode job to bytes
func (job Job) Encode() []byte {
	var j types.Job
	j.Func = job.Func
	j.Name = job.Name
	j.Args = job.Args
	j.SchedAt = job.SchedAt
	j.Counter = job.Counter
	return j.Bytes()
}
