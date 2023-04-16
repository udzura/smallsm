package main

import (
	"fmt"
	"os"

	"github.com/udzura/smallsm"
)

func main() {
	db, err := smallsm.New(os.TempDir() + "/smallsm-1")
	if err != nil {
		panic(err)
	}

	db.Put("Test", "Hello")
	db.Put("Test2", "Hello2")
	db.Put("Test3", "Hello3")

	got, _ := db.Get("Test")
	fmt.Println("Test =", got)
	got, _ = db.Get("Test2")
	fmt.Println("Test2 =", got)
	got, _ = db.Get("Test3")
	fmt.Println("Test3 =", got)

	if err := db.MigrateToSSTable(); err != nil {
		panic(err)
	}

	got, _ = db.Get("Test")
	fmt.Println("Test =", got)
	got, _ = db.Get("Test2")
	fmt.Println("Test2 =", got)
	got, _ = db.Get("Test3")
	fmt.Println("Test3 =", got)

	db.Delete("Test3")
	got, _ = db.Get("Test3")
	fmt.Println("Test3 =", got)

	if err := db.MigrateToSSTable(); err != nil {
		panic(err)
	}

	got, _ = db.Get("Test3")
	fmt.Println("Test3 =", got)

	// reopem
	db, _ = smallsm.New(os.TempDir() + "/smallsm-1")
	got, _ = db.Get("Test")
	fmt.Println("Test =", got)
	got, _ = db.Get("Test2")
	fmt.Println("Test2 =", got)
	got, _ = db.Get("Test3")
	fmt.Println("Test3 =", got)
}
