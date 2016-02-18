package subcmd

import (
	"github.com/Lupino/go-periodic"
	"log"
	"os"
)

// Dump cli dump
func Dump(entryPoint, output string) {
	c := periodic.NewClient()
	if err := c.Connect(entryPoint); err != nil {
		log.Fatal(err)
	}
	var fp *os.File
	var err error
	if fp, err = os.Create(output); err != nil {
		log.Fatal(err)
	}

	defer fp.Close()

	c.Dump(fp)
}
