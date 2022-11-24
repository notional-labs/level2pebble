package main

import (
	"fmt"
	//"github.com/cockroachdb/pebble"
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
	//dbDirTarget := os.Args[2]

	// options to disable compaction for goleveldb
	levelOptions := levelopt.Options{
		CompactionL0Trigger:    math.MaxInt32,
		DisableSeeksCompaction: true,
		WriteL0PauseTrigger:    math.MaxInt32,
		WriteL0SlowdownTrigger: math.MaxInt32,
		OpenFilesCacheCapacity: 10,
		//BlockCacheCapacity:     -1,
		BlockCacheEvictRemoved: true,
		DisableBlockCache:      true,
		ReadOnly:               true,
	}
	dbLev, errLev := tmdb.NewGoLevelDBWithOpts(dbName, dbDirSource, &levelOptions)
	if errLev != nil {
		panic(errLev)
	}

	//// options to disable compaction for pebbledb
	//pebbleOptions := &pebble.Options{
	//	L0CompactionFileThreshold:   math.MaxInt32,
	//	L0CompactionThreshold:       math.MaxInt32,
	//	L0StopWritesThreshold:       math.MaxInt32,
	//	MaxConcurrentCompactions:    1,
	//	DisableAutomaticCompactions: true,
	//	MaxOpenFiles:                10,
	//}
	//pebbleOptions.Experimental.ReadCompactionRate = math.MaxInt32
	//pebbleOptions.EnsureDefaults()
	//dbPeb, errPeb := tmdb.NewPebbleDBWithOpts(dbName, dbDirTarget, pebbleOptions)
	//
	//if errPeb != nil {
	//	panic(errPeb)
	//}

	defer func() {
		//dbPeb.Close()
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

	//rawDBPebble := dbPeb.DB()
	//bat := rawDBPebble.NewBatch()

	for itr.First(); itr.Valid(); itr.Next() {
		offset++

		key := cp(itr.Key())
		//value := cp(itr.Value())

		if offset%1000000 == 0 {
			fmt.Printf("reading %s: %d\n", dbName, offset)
			// release itr and create the new one to see if mem usage will be lower
			itr.Release()
			itr = dbLev.DB().NewIterator(&util.Range{Start: key, Limit: nil}, &readOptions)
			itr.First()

			runtime.GC() // Force GC
		}

		//errSet := bat.Set(key, value, pebble.Sync)
		//if errSet != nil {
		//	panic(errSet)
		//}
		//
		//if bat.Len() >= 107374182 { // 100 MB
		//	fmt.Printf("processing %s: %d\n", dbName, offset)
		//
		//	bat.Commit(pebble.Sync)
		//	bat.Reset()
		//	rawDBPebble.Flush()
		//
		//	runtime.GC() // Force GC
		//
		//	// release itr and create the new one to see if mem usage will be lower
		//	itr.Release()
		//	itr = dbLev.DB().NewIterator(&util.Range{Start: key, Limit: nil}, &readOptions)
		//	itr.First()
		//	//itr.Next()
		//}
	}

	//// write the last batch
	//bat.Commit(pebble.Sync)
	//bat.Close()
	//rawDBPebble.Flush()
}

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}
