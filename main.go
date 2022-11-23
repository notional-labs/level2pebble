package main

import (
	"fmt"
	"github.com/cockroachdb/pebble"
	levelopt "github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
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
		OpenFilesCacheCapacity: 100,
	}
	dbLev, errLev := tmdb.NewGoLevelDBWithOpts(dbName, dbDirSource, &levelOptions)
	if errLev != nil {
		panic(errLev)
	}

	// options to disable compaction for pebbledb
	pebbleOptions := &pebble.Options{
		L0CompactionFileThreshold:   math.MaxInt32,
		L0CompactionThreshold:       math.MaxInt32,
		L0StopWritesThreshold:       math.MaxInt32,
		MaxConcurrentCompactions:    1,
		DisableAutomaticCompactions: true,
		MaxOpenFiles:                100,
	}
	pebbleOptions.Experimental.ReadCompactionRate = math.MaxInt32
	pebbleOptions.EnsureDefaults()
	dbPeb, errPeb := tmdb.NewPebbleDBWithOpts(dbName, dbDirTarget, pebbleOptions)

	if errPeb != nil {
		panic(errPeb)
	}

	defer func() {
		dbPeb.Close()
		dbLev.Close()
	}()

	//itr, itrErr := dbLev.Iterator(nil, nil)
	//if itrErr != nil {
	//	panic(itrErr)
	//}

	readOptions := levelopt.ReadOptions{
		DontFillCache: true,
		Strict:        levelopt.DefaultStrict,
	}
	itr := dbLev.DB().NewIterator(&util.Range{Start: nil, Limit: nil}, &readOptions)

	defer func() {
		// itr.Close()
		itr.Release()
	}()

	offset := 0

	rawDBPebble := dbPeb.DB()
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
	}

	// write the last batch
	bat.Commit(pebble.Sync)
	bat.Close()
}
