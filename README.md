# level2pebble

Convert GoLevelDB database into PebbleDB database

### Build
```bash
make build
```

### Install
```bash
make install
```

### Usage
```bash
level2pebble <sourcePath> <targetDir>
```

- `<sourcePath>`: path to goleveldb database
- `<targetDir>`: dir to pebbledb database

Example:

```
level2pebble /Volumes/RAMDisk/test/data/state.db /Volumes/RAMDisk/pebbledb
```

will convert `/Volumes/RAMDisk/test/data/state.db` goleveldb to new pebbledb with path `/Volumes/RAMDisk/pebbledb/state.db`