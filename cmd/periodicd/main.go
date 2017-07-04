package main

import (
	"github.com/Lupino/periodic"
	"github.com/Lupino/periodic/driver"
	"github.com/Lupino/periodic/driver/leveldb"
	"github.com/Lupino/periodic/driver/redis"
	"github.com/urfave/cli"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"time"
)

func main() {
	app := cli.NewApp()
	app.Name = "periodic"
	app.Usage = "Periodic task system"
	app.Version = periodic.Version
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "H",
			Value:  "unix:///tmp/periodic.sock",
			Usage:  "the server address eg: tcp://127.0.0.1:5000",
			EnvVar: "PERIODIC_PORT",
		},
		cli.StringFlag{
			Name:  "redis",
			Value: "tcp://127.0.0.1:6379",
			Usage: "The redis server address, required for driver redis",
		},
		cli.StringFlag{
			Name:  "driver",
			Value: "memstore",
			Usage: "The driver [memstore, leveldb, redis]",
		},
		cli.StringFlag{
			Name:  "dbpath",
			Value: "leveldb",
			Usage: "The db path, required for driver leveldb",
		},
		cli.IntFlag{
			Name:  "timeout",
			Value: 0,
			Usage: "The socket timeout",
		},
		cli.IntFlag{
			Name:   "cpus",
			Value:  runtime.NumCPU(),
			Usage:  "The runtime.GOMAXPROCS",
			EnvVar: "GOMAXPROCS",
		},
		cli.StringFlag{
			Name:  "cpuprofile",
			Value: "",
			Usage: "write cpu profile to file",
		},
	}
	app.Action = func(c *cli.Context) error {
		if c.String("cpuprofile") != "" {
			f, err := os.Create(c.String("cpuprofile"))
			if err != nil {
				log.Fatal(err)
			}
			pprof.StartCPUProfile(f)
			defer pprof.StopCPUProfile()
		}
		var store driver.StoreDriver
		switch c.String("driver") {
		case "memstore":
			store = driver.NewMemStroeDriver()
			break
		case "redis":
			store = redis.NewDriver(c.String("redis"))
			break
		case "leveldb":
			store = leveldb.NewDriver(c.String("dbpath"))
			break
		default:
			store = driver.NewMemStroeDriver()
			break
		}

		runtime.GOMAXPROCS(c.Int("cpus"))
		timeout := time.Duration(c.Int("timeout"))
		periodicd := periodic.NewSched(c.String("H"), store, timeout)
		go periodicd.Serve()
		s := make(chan os.Signal, 1)
		signal.Notify(s, os.Interrupt, os.Kill)
		<-s
		periodicd.Close()
		return nil
	}

	app.Run(os.Args)
}
