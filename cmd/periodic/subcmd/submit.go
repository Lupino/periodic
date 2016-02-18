package subcmd

import (
	"github.com/Lupino/go-periodic"
	"github.com/Lupino/periodic/driver"
	"log"
)

// SubmitJob cli submit
func SubmitJob(entryPoint string, job driver.Job) {
	c := periodic.NewClient()
	if err := c.Connect(entryPoint); err != nil {
		log.Fatal(err)
	}
	if err := c.SubmitJob(job); err != nil {
		log.Fatal(err)
	}
	log.Printf("Submit Job[%s] success.\n", job.Name)
}
