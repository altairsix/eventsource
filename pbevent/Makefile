.PHONY: build

build:
	protoc -I=. --go_out=. --go_opt=paths=source_relative \
	    ./event.proto
