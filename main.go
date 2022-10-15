package main

import (
	"fmt"
	tmdb "github.com/tendermint/tm-db"
	"os"
	"path/filepath"
	"runtime"
)

func main() {
	if len(os.Args) != 3 {
		panic("Usage: level2pebble <sourcePath> <targetDir>")
	}

	fmt.Printf("pebble2level: source=%s, target=%s\n", os.Args[1], os.Args[2])

	dbName := filepath.Base(os.Args[1])
	dbName = dbName[:len(dbName)-len(filepath.Ext(dbName))] // get rid of .db
	dbDirSource := filepath.Dir(os.Args[1])
	dbDirTarget := os.Args[2]

	dbLev, errLev := tmdb.NewPebbleDB(dbName, dbDirSource)
	if errLev != nil {
		panic(errLev)
	}

	dbPeb, errPeb := tmdb.NewGoLevelDB(dbName, dbDirTarget)

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

		if offset%1000000 == 0 {
			fmt.Printf("processing %s: %d\n", dbName, offset)
			runtime.GC() // Force GC
		}
	}

	itr.Close()
}
