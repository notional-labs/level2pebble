package main

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	tmdb "github.com/tendermint/tm-db"
	"os"
	"path/filepath"
	"runtime"
)

func main() {
	if len(os.Args) != 3 {
		panic("Usage: level2pebble <sourcePath> <targetDir>")
	}

	fmt.Printf("source=%s, target=%s\n", os.Args[1], os.Args[2])

	dbName := filepath.Base(os.Args[1])
	dbName = dbName[:len(dbName)-len(filepath.Ext(dbName))] // get rid of .db
	dbDirSource := filepath.Dir(os.Args[1])
	dbDirTarget := os.Args[2]

	dbLev, errLev := tmdb.NewGoLevelDB(dbName, dbDirSource)
	if errLev != nil {
		panic(errLev)
	}

	dbPeb, errPeb := tmdb.NewPebbleDB(dbName, dbDirTarget)

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

	rawDBPebble := dbPeb.(*tmdb.PebbleDB).DB()
	bat := rawDBPebble.NewBatch()

	for ; itr.Valid(); itr.Next() {
		key := itr.Key()
		value := itr.Value()

		offset++

		errSet := bat.Set(key, value, pebble.Sync)
		if errSet != nil {
			panic(errSet)
		}

		if offset%30000 == 0 {
			fmt.Printf("processing %s: %d\n", dbName, offset)

			bat.Commit(pebble.Sync)
			bat.Reset()

			runtime.GC() // Force GC
		}

		// for testing only
		//if offset > 15000 {
		//	break
		//}
	}

	// write the last batch
	bat.Commit(pebble.Sync)
	bat.Close()

	itr.Close()
}
