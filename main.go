package main

import (
	"encoding/hex"
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
		OpenFilesCacheCapacity: -1,
		BlockCacheCapacity:     -1,
		BlockCacheEvictRemoved: true,
		DisableBufferPool:      true,
		DisableBlockCache:      true,
		ReadOnly:               true,
		OpenFilesCacher:        levelopt.NoCacher,
		BlockCacher:            levelopt.NoCacher,
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

	// reading tx_index: 5294000000, key=ffd1e16a90b7b05050324904fa3c05c996da4833d3b4d128bfb95d7b658e0584
	start_key, errDecode := hex.DecodeString("ffd1e16a90b7b05050324904fa3c05c996da4833d3b4d128bfb95d7b658e0584")
	if errDecode != nil {
		panic(errDecode)
	}

	itr := dbLev.DB().NewIterator(&util.Range{Start: start_key, Limit: nil}, &readOptions)

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

		if offset%100000 == 0 {
			str_hex_key := hex.EncodeToString(key)
			fmt.Printf("reading %s: %d, key=%s\n", dbName, offset, str_hex_key)
			// release itr and create the new one to see if mem usage will be lower
			itr.Release()

			//// close the db and reopen it
			//dbLev.Close()
			//dbLev, errLev = tmdb.NewGoLevelDBWithOpts(dbName, dbDirSource, &levelOptions)
			//if errLev != nil {
			//	panic(errLev)
			//}

			runtime.GC() // Force GC

			itr = dbLev.DB().NewIterator(&util.Range{Start: key, Limit: nil}, &readOptions)
			itr.First()

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
		//
		//	// release itr and create the new one to see if mem usage will be lower
		//	itr.Release()
		//
		//	runtime.GC() // Force GC
		//
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
