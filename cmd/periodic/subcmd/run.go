package subcmd


import (
    "net"
    "strings"
    "github.com/Lupino/periodic"
    "github.com/Lupino/periodic/driver"
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
        conn := periodic.Conn{Conn: c}
        err = handleWorker(conn, Func, cmd)
        if err != nil {
            if err != io.EOF {
                log.Printf("Error: %s\n", err.Error())
            }
        }
        conn.Close()
    }
}


func handleWorker(conn periodic.Conn, Func, cmd string) (err error) {
    err = conn.Send(periodic.TYPE_WORKER.Bytes())
    if err != nil {
        return
    }
    var msgId = []byte("100")
    buf := bytes.NewBuffer(nil)
    buf.Write(msgId)
    buf.Write(periodic.NULL_CHAR)
    buf.WriteByte(byte(periodic.CAN_DO))
    buf.Write(periodic.NULL_CHAR)
    buf.WriteString(Func)
    err = conn.Send(buf.Bytes())
    if err != nil {
        return
    }

    var payload []byte
    var job driver.Job
    var jobHandle []byte
    for {
        buf = bytes.NewBuffer(nil)
        buf.Write(msgId)
        buf.Write(periodic.NULL_CHAR)
        buf.Write(periodic.GRAB_JOB.Bytes())
        err = conn.Send(buf.Bytes())
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
        buf = bytes.NewBuffer(nil)
        buf.Write(msgId)
        buf.Write(periodic.NULL_CHAR)
        if err != nil || fail {
            buf.WriteByte(byte(periodic.JOB_FAIL))
        } else if schedLater > 0 {
            buf.WriteByte(byte(periodic.SCHED_LATER))
        } else {
            buf.WriteByte(byte(periodic.JOB_DONE))
        }
        buf.Write(periodic.NULL_CHAR)
        buf.Write(jobHandle)
        if schedLater > 0 {
            buf.Write(periodic.NULL_CHAR)
            buf.WriteString(strconv.Itoa(schedLater))
        }
        err = conn.Send(buf.Bytes())
        if err != nil {
            return
        }
    }
}


func extraJob(payload []byte) (job driver.Job, jobHandle []byte, err error) {
    parts := bytes.SplitN(payload, periodic.NULL_CHAR, 3)
    if len(parts) != 3 {
        err = errors.New("Invalid payload " + string(payload))
        return
    }
    job, err = driver.NewJob(parts[2])
    jobHandle = parts[1]
    return
}