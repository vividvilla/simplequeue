.PHONY: build test test-race lint clean

build:
	go build ./...
	go build -o bin/simplequeued ./cmd/simplequeued
	go build -o bin/simplequeue ./cmd/simplequeue

test:
	go test ./...

test-race:
	go test -race ./...

test-integration:
	go test -tags integration ./...

lint:
	go vet ./...

clean:
	rm -rf bin/
