package main

import (
    "log"
    "net"
    "time"
    "sync"
    "strings"
    "container/list"
    "huabot-sched/db"
)


type Sched struct {
    TotalWorkerCount int
    timer            *time.Timer
    grabQueue        *list.List
    jobQueue         *list.List
    entryPoint       string
    JobLocker        *sync.Mutex
    Funcs            map[string]*FuncStat
}


type FuncStat struct {
    TotalWorker uint `json:"worker_count"`
    TotalJob    uint `json:"job_count"`
    ProcJob     uint `json:"processing"`
}


func (stat *FuncStat) IncrWorker() uint {
    stat.TotalWorker += 1
    return stat.TotalWorker
}


func (stat *FuncStat) DecrWorker() uint {
    stat.TotalWorker -= 1
    return stat.TotalWorker
}


func (stat *FuncStat) IncrJob() uint {
    stat.TotalJob += 1
    return stat.TotalJob
}


func (stat *FuncStat) DecrJob() uint {
    stat.TotalJob -= 1
    return stat.TotalJob
}


func (stat *FuncStat) IncrProc() uint {
    stat.ProcJob += 1
    return stat.ProcJob
}


func (stat *FuncStat) DecrProc() uint {
    stat.ProcJob -= 1
    return stat.ProcJob
}


func NewSched(entryPoint string) *Sched {
    sched = new(Sched)
    sched.TotalWorkerCount = 0
    sched.timer = time.NewTimer(1 * time.Hour)
    sched.grabQueue = list.New()
    sched.jobQueue = list.New()
    sched.entryPoint = entryPoint
    sched.JobLocker = new(sync.Mutex)
    sched.Funcs = make(map[string]*FuncStat)
    return sched
}


func (sched *Sched) Serve() {
    parts := strings.SplitN(sched.entryPoint, "://", 2)
    if parts[0] == "unix" {
        sockCheck(parts[1])
    }
    sched.checkJobQueue()
    go sched.handle()
    listen, err := net.Listen(parts[0], parts[1])
    if err != nil {
        log.Fatal(err)
    }
    defer listen.Close()
    log.Printf("huabot-sched started on %s\n", sched.entryPoint)
    for {
        conn, err := listen.Accept()
        if err != nil {
            log.Fatal(err)
        }
        sched.HandleConnection(conn)
    }
}


func (sched *Sched) Notify() {
    sched.timer.Reset(time.Millisecond)
}


func (sched *Sched) DieWorker(worker *Worker) {
    defer sched.Notify()
    sched.TotalWorkerCount -= 1
    log.Printf("Total worker: %d\n", sched.TotalWorkerCount)
    sched.removeGrabQueue(worker)
    worker.Close()
}

func (sched *Sched) HandleConnection(conn net.Conn) {
    worker := NewWorker(sched, Conn{Conn: conn})
    sched.TotalWorkerCount += 1
    log.Printf("Total worker: %d\n", sched.TotalWorkerCount)
    go worker.Handle()
}


func (sched *Sched) Done(jobId int64) {
    defer sched.Notify()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    removeListJob(sched.jobQueue, jobId)
    job, err := db.GetJob(jobId)
    if err == nil {
        job.Delete()
        sched.DecrStatJob(job)
        sched.DecrStatProc(job)
    }
    return
}


func (sched *Sched) isDoJob(job db.Job) bool {
    now := time.Now()
    current := int64(now.Unix())
    ret := false
    for e := sched.jobQueue.Front(); e != nil; e = e.Next() {
        chk := e.Value.(db.Job)
        runAt := chk.RunAt
        if runAt < chk.SchedAt {
            runAt = chk.SchedAt
        }
        if chk.Timeout > 0 && runAt + chk.Timeout < current {
            newJob, _ := db.GetJob(chk.Id)
            if newJob.Status == db.JOB_STATUS_PROC {
                sched.DecrStatProc(newJob)
                newJob.Status = db.JOB_STATUS_READY
                newJob.Save()
            }
            sched.jobQueue.Remove(e)
            continue
        }
        if chk.Id == job.Id {
            old := e.Value.(db.Job)
            runAt := old.RunAt
            if runAt < old.SchedAt {
                runAt = old.SchedAt
            }
            if old.Timeout > 0 && runAt + old.Timeout < current {
                ret = false
            } else {
                ret = true
            }
        }
    }
    return ret
}


func (sched *Sched) SubmitJob(worker *Worker, job db.Job) {
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    if job.Name == "" {
        job.Delete()
        return
    }
    if sched.isDoJob(job) {
        return
    }
    if !worker.alive {
        return
    }
    if err := worker.HandleDo(job); err != nil {
        worker.alive = false
        go sched.DieWorker(worker)
        return
    }
    now := time.Now()
    current := int64(now.Unix())
    job.Status = db.JOB_STATUS_PROC
    job.RunAt = current
    job.Save()
    sched.IncrStatProc(job)
    sched.jobQueue.PushBack(job)
    sched.removeGrabQueue(worker)
}


func (sched *Sched) handle() {
    var current time.Time
    var timestamp int64
    var schedJob db.Job
    var isFirst bool
    for {
        if sched.grabQueue.Len() == 0 {
            sched.timer.Reset(time.Minute)
            current =<-sched.timer.C
            continue
        }

        isFirst = true
        for Func, stat := range sched.Funcs {
            if stat.TotalWorker == 0 || (stat.TotalJob > 0 && stat.ProcJob == stat.TotalJob) {
                continue
            }
            jobs, err := db.RangeSchedJob(Func, db.JOB_STATUS_READY, 0, 0)
            if err != nil || len(jobs) == 0 {
                stat.TotalJob = stat.ProcJob
                continue
            }

            if isFirst {
                schedJob = jobs[0]
                isFirst = false
                continue
            }

            if schedJob.SchedAt > jobs[0].SchedAt {
                schedJob = jobs[0]
            }
        }
        if isFirst {
            sched.timer.Reset(time.Minute)
            current =<-sched.timer.C
            continue
        }

        timestamp = int64(time.Now().Unix())

        if schedJob.SchedAt > timestamp {
            sched.timer.Reset(time.Second * time.Duration(schedJob.SchedAt - timestamp))
            current =<-sched.timer.C
            timestamp = int64(current.Unix())
            if schedJob.SchedAt > timestamp {
                continue
            }
        }

        isSubmited := false
        for e := sched.grabQueue.Front(); e != nil; e = e.Next() {
            worker := e.Value.(*Worker)
            for _, Func := range worker.Funcs {
                if schedJob.Func == Func {
                    sched.SubmitJob(worker, schedJob)
                    isSubmited = true
                    break
                }
            }
            if isSubmited {
                break
            }
        }

        if !isSubmited {
            sched.DecrStatFunc(schedJob.Func)
        }
    }
}


func (sched *Sched) Fail(jobId int64) {
    defer sched.Notify()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    removeListJob(sched.jobQueue, jobId)
    job, _ := db.GetJob(jobId)
    sched.DecrStatProc(job)
    job.Status = db.JOB_STATUS_READY
    job.Save()
    return
}


func (sched *Sched) IncrStatFunc(Func string) {
    stat, ok := sched.Funcs[Func]
    if !ok {
        stat = new(FuncStat)
        sched.Funcs[Func] = stat
    }
    stat.IncrWorker()
}


func (sched *Sched) DecrStatFunc(Func string) {
    stat, ok := sched.Funcs[Func]
    if ok {
        stat.DecrWorker()
    }
}


func (sched *Sched) IncrStatJob(job db.Job) {
    stat, ok := sched.Funcs[job.Func]
    if !ok {
        stat = new(FuncStat)
        sched.Funcs[job.Func] = stat
    }
    stat.IncrJob()
}


func (sched *Sched) DecrStatJob(job db.Job) {
    stat, ok := sched.Funcs[job.Func]
    if ok {
        stat.DecrJob()
    }
}


func (sched *Sched) IncrStatProc(job db.Job) {
    stat, ok := sched.Funcs[job.Func]
    if !ok {
        stat = new(FuncStat)
        sched.Funcs[job.Func] = stat
    }
    if job.Status == db.JOB_STATUS_PROC {
        stat.IncrProc()
    }
}


func (sched *Sched) DecrStatProc(job db.Job) {
    stat, ok := sched.Funcs[job.Func]
    if ok && job.Status == db.JOB_STATUS_PROC {
        stat.DecrProc()
    }
}


func (sched *Sched) SchedLater(jobId int64, delay int64) {
    defer sched.Notify()
    defer sched.JobLocker.Unlock()
    sched.JobLocker.Lock()
    removeListJob(sched.jobQueue, jobId)
    job, _ := db.GetJob(jobId)
    sched.DecrStatProc(job)
    job.Status = db.JOB_STATUS_READY
    var now = time.Now()
    job.SchedAt = int64(now.Unix()) + delay
    job.Save()
    return
}


func (sched *Sched) removeGrabQueue(worker *Worker) {
    for e := sched.grabQueue.Front(); e != nil; e = e.Next() {
        if e.Value.(*Worker) == worker {
            sched.grabQueue.Remove(e)
        }
    }
}


func (sched *Sched) checkJobQueue() {
    start := 0
    limit := 20
    total, _ := db.CountJob()
    updateQueue := make([]db.Job, 0)
    removeQueue := make([]db.Job, 0)
    var now = time.Now()
    current := int64(now.Unix())

    for start = 0; start < int(total); start += limit {
        jobs, _ := db.RangeJob(start, start + limit - 1)
        for _, job := range jobs {
            if job.Name == "" {
                removeQueue = append(removeQueue, job)
                continue
            }
            sched.IncrStatJob(job)
            if job.Status != db.JOB_STATUS_PROC {
                continue
            }
            runAt := job.RunAt
            if runAt < job.SchedAt {
                runAt = job.SchedAt
            }
            if runAt + job.Timeout < current {
                updateQueue = append(updateQueue, job)
            } else {
                sched.jobQueue.PushBack(job)
                sched.IncrStatProc(job)
            }
        }
    }

    for _, job := range updateQueue {
        job.Status = db.JOB_STATUS_READY
        job.Save()
    }

    for _, job := range removeQueue {
        job.Delete()
    }
}


func (sched *Sched) Close() {
}
