BINARY := andamio
GOFLAGS := -trimpath

.PHONY: build test race vet fmt docker clean

build:
	go build $(GOFLAGS) -o bin/$(BINARY) ./cmd/andamio

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

docker:
	docker build -t edxeme/andamio:dev -f deploy/docker/Dockerfile .

clean:
	rm -rf bin/
