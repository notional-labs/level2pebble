package main

import (
	"fmt"
	tmdb "github.com/tendermint/tm-db"
	"os"
	"runtime"
)

func main() {
	if len(os.Args) != 3 {
		panic("Usage: level2pebble <source> <target>")
	}

	fmt.Printf("source=%s, target=%s\n", os.Args[1], os.Args[2])

	dbLev, errLev := tmdb.NewGoLevelDB(os.Args[2], os.Args[1])
	if errLev != nil {
		panic(errLev)
	}

	dbPeb, errPeb := tmdb.NewPebbleDB(os.Args[2], ".")

	if errPeb != nil {
		panic(errPeb)
	}

	defer func() {
		dbPeb.Close()
		dbLev.Close()
	}()

	itr, itrErr := dbLev.Iterator(nil, nil)

	if itrErr != nil {
		panic(itrErr)
	}

	offset := 0

	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()

		errSet := dbPeb.Set(key, value)
		if errSet != nil {
			panic(errSet)
		}

		offset++

		if offset%10000 == 0 {
			fmt.Println(offset)
			runtime.GC() // Force GC
		}
	}

	itr.Close()
}
