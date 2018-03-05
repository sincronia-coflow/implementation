all: sincronia.capnp.go
	go build .

capnp: sincronia.capnp.go

sincronia.capnp.go:
	capnp compile -I${GOPATH}/src/zombiezen.com/go/capnproto2/std -ogo ../sincronia.capnp
	mkdir -p ./scheduler
	mv ../sincronia.capnp.go ./scheduler/

clean:
	rm -rf ./scheduler
