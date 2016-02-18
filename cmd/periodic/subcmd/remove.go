package subcmd

import (
	"github.com/Lupino/go-periodic"
	"github.com/Lupino/periodic/driver"
	"log"
)

// RemoveJob cli remove
func RemoveJob(entryPoint string, job driver.Job) {
	c := periodic.NewClient()
	if err := c.Connect(entryPoint); err != nil {
		log.Fatal(err)
	}
	if err := c.RemoveJob(job); err != nil {
		log.Fatal(err)
	}
	log.Printf("Remove Job[%s] success.\n", job.Name)
}
