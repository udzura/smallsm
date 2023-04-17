package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/udzura/smallsm"
)

func main() {
	db, err := smallsm.New(os.TempDir() + "/smallsm-2")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 1024; i++ {
		suffix1 := strconv.FormatInt(int64(i), 10)
		suffix2 := strconv.FormatInt(int64(i), 16)
		db.Put("Record-"+suffix1, "Value="+suffix2)
	}
	if err := db.MigrateToSSTable(); err != nil {
		panic(err)
	}

	for i := 0; i < 128; i++ {
		suffix := strconv.FormatInt(int64(i*3), 16)
		db.Put("Record-"+suffix, "Value updated="+suffix)
	}
	if err := db.MigrateToSSTable(); err != nil {
		panic(err)
	}

	for i := 128; i < 256; i++ {
		suffix := strconv.FormatInt(int64(i*3), 16)
		db.Delete("Record-" + suffix)
	}
	if err := db.MigrateToSSTable(); err != nil {
		panic(err)
	}

	for i := 0; i < 1024; i++ {
		suffix := strconv.FormatInt(int64(i), 10)
		v, ok := db.Get("Record-" + suffix)
		if !ok {
			fmt.Println("Record-"+suffix, "is deleted")
			continue
		}
		fmt.Println("Record-"+suffix, "=", v)
	}
}
