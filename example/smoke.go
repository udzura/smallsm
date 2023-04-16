package main

import (
	"fmt"
	"os"

	"github.com/udzura/smallsm"
)

func main() {
	db := smallsm.New(os.TempDir() + "/smallsm-1")

	db.Put("Test", "Hello")
	db.Put("Test2", "Hello2")
	db.Put("Test3", "Hello3")

	got, _ := db.Get("Test")
	fmt.Println("Test =", got)
}
