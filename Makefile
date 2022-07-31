build:
	go build -tags pebbledb -o level2pebble ./main.go

install:
	go install -tags pebbledb  ./...