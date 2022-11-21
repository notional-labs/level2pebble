package main

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	levelopt "github.com/syndtr/goleveldb/leveldb/opt"
	tmdb "github.com/tendermint/tm-db"
	"math"
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

	// options to disable compaction for goleveldb
	levelOptions := levelopt.Options{
		CompactionL0Trigger:    math.MaxInt32,
		DisableSeeksCompaction: true,
		WriteL0PauseTrigger:    math.MaxInt32,
		WriteL0SlowdownTrigger: math.MaxInt32,
	}
	dbLev, errLev := tmdb.NewGoLevelDBWithOpts(dbName, dbDirSource, &levelOptions)
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

		if bat.Len() >= 1073741824 { // 1 GB
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
