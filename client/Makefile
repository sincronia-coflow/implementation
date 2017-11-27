all: sinchronia.capnp.go
	go build .

capnp: sinchronia.capnp.go

sinchronia.capnp.go:
	capnp compile -I${GOPATH}/src/zombiezen.com/go/capnproto2/std -ogo ../sinchronia.capnp
	mkdir -p ./scheduler
	mv ../sinchronia.capnp.go ./scheduler/

clean:
	rm -rf ./scheduler
