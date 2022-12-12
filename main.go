package main

import (
	"encoding/hex"
	"fmt"
	"github.com/cockroachdb/pebble"
	"math"

	levelopt "github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
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

	// options to disable compaction for goleveldb
	levelOptions := levelopt.Options{
		ReadOnly: true,
		//CompactionTableSizeMultiplier: 2.0,
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

	for itr.First(); itr.Valid(); itr.Next() {
		offset++

		key := cp(itr.Key())
		value := cp(itr.Value())

		errSet := bat.Set(key, value, pebble.Sync)
		if errSet != nil {
			panic(errSet)
		}

		if bat.Len() >= 107374182 { // 100 MB
			str_hex_key := hex.EncodeToString(key)
			fmt.Printf("processing %s: %d, key=%s\n", dbName, offset, str_hex_key)

			errComit := bat.Commit(pebble.Sync)
			if errComit != nil {
				panic(errComit)
			}

			errFlush := rawDBPebble.Flush()
			if errFlush != nil {
				panic(errFlush)
			}

			bat.Reset()

			runtime.GC() // Force GC
		}
	}

	// write the last batch
	errComit := bat.Commit(pebble.Sync)
	if errComit != nil {
		panic(errComit)
	}

	errFlush := rawDBPebble.Flush()
	if errFlush != nil {
		panic(errFlush)
	}

	bat.Close()

	fmt.Printf("Done!")
}

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}
