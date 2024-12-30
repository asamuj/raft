package main

import (
	"fmt"

	"github.com/cockroachdb/pebble"
)

func main() {
	db, err := pebble.Open("/tmp/meta3", &pebble.Options{})
	if err != nil {
		panic(err)
	}

	iter, err := db.NewIter(nil)
	if err != nil {
		panic(err)
	}

	for iter.First(); iter.Valid(); iter.Next() {
		fmt.Printf("Key: %s, Value: %s\n", iter.Key(), iter.Value())
	}
}
