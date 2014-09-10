package cmd


import (
    "net"
    "strings"
    "periodic/sched"
    "fmt"
    "log"
    "bytes"
    "errors"
    "io"
    "os"
    "os/exec"
    "strconv"
    "time"
)


func Run(entryPoint, Func, cmd string) {
    parts := strings.SplitN(entryPoint, "://", 2)
    for {
        c, err := net.Dial(parts[0], parts[1])
        if err != nil {
            if err != io.EOF {
                log.Printf("Error: %s\n", err.Error())
            }
            log.Printf("Wait 5 second to reconnecting")
            time.Sleep(5 * time.Second)
            continue
        }
        conn := sched.Conn{Conn: c}
        err = handleWorker(conn, Func, cmd)
        if err != nil {
            if err != io.EOF {
                log.Printf("Error: %s\n", err.Error())
            }
        }
        conn.Close()
    }
}


func handleWorker(conn sched.Conn, Func, cmd string) (err error) {
    err = conn.Send(sched.TYPE_WORKER.Bytes())
    if err != nil {
        return
    }
    buf := bytes.NewBuffer(nil)
    buf.WriteByte(byte(sched.CAN_DO))
    buf.Write(sched.NULL_CHAR)
    buf.WriteString(Func)
    err = conn.Send(buf.Bytes())
    if err != nil {
        return
    }

    var payload []byte
    var job sched.Job
    var jobHandle []byte
    for {
        err = conn.Send(sched.GRAB_JOB.Bytes())
        if err != nil {
            return
        }
        payload, err = conn.Receive()
        if err != nil {
            return
        }
        job, jobHandle, err = extraJob(payload)
        realCmd := strings.Split(cmd, " ")
        realCmd = append(realCmd, job.Name)
        c := exec.Command(realCmd[0], realCmd[1:]...)
        c.Stdin = strings.NewReader(job.Args)
        var out bytes.Buffer
        c.Stdout = &out
        c.Stderr = os.Stderr
        err = c.Run()
        var schedLater int
        var fail = false
        for {
            line, err := out.ReadString([]byte("\n")[0])
            if err != nil {
                break
            }
            if strings.HasPrefix(line, "SCHED_LATER") {
                parts := strings.SplitN(line[:len(line) - 1], " ", 2)
                later := strings.Trim(parts[1], " ")
                schedLater, _ = strconv.Atoi(later)
            } else if strings.HasPrefix(line, "FAIL") {
                fail = true
            } else {
                fmt.Print(line)
            }
        }
        buf := bytes.NewBuffer(nil)
        if err != nil || fail {
            buf.WriteByte(byte(sched.JOB_FAIL))
        } else if schedLater > 0 {
            buf.WriteByte(byte(sched.SCHED_LATER))
        } else {
            buf.WriteByte(byte(sched.JOB_DONE))
        }
        buf.Write(sched.NULL_CHAR)
        buf.Write(jobHandle)
        if schedLater > 0 {
            buf.Write(sched.NULL_CHAR)
            buf.WriteString(strconv.Itoa(schedLater))
        }
        err = conn.Send(buf.Bytes())
        if err != nil {
            return
        }
    }
}


func extraJob(payload []byte) (job sched.Job, jobHandle []byte, err error) {
    parts := bytes.SplitN(payload, sched.NULL_CHAR, 2)
    if len(parts) != 2 {
        err = errors.New("Invalid payload " + string(payload))
        return
    }
    job, err = sched.NewJob(parts[0])
    jobHandle = parts[1]
    return
}
