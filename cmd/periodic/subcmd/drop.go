package subcmd

import (
	"github.com/Lupino/go-periodic"
	"log"
)

// DropFunc cli drop
func DropFunc(entryPoint, funcName string) {
	c := periodic.NewClient()
	if err := c.Connect(entryPoint); err != nil {
		log.Fatal(err)
	}
	if err := c.DropFunc(funcName); err != nil {
		log.Fatal(err)
	}
	log.Printf("Drop Func[%s] success.\n", funcName)
}
