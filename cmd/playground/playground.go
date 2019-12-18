package main

import (
	"log"
	"os"
	"time"

	"github.com/fterrag/simple-sqsd/sqsworker"
)

func main() {
	l := log.New(os.Stdout, "", 0)

	d := sqsworker.NewDirector(l)
	d.Start(2)

	time.Sleep(2 * time.Second)

	d.Shutdown()
	d.Wait()
}
