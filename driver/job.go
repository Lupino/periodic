package driver

import (
	"encoding/json"
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
