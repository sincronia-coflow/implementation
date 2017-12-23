all: scheduler

scheduler: sinchronia.capnp.h main.cpp
	g++ -o sinchronia-scheduler -std=c++14 sinchronia.capnp.c++ main.cpp -lcapnp -lcapnp-rpc -lkj -lkj-async -g -gdwarf-2

sinchronia.capnp.h:
	capnp compile -I${GOPATH}/src/zombiezen.com/go/capnproto2/std -oc++ ../sinchronia.capnp
	mv ../sinchronia.capnp.h ./
	mv ../sinchronia.capnp.c++ ./

capnp: sinchronia.capnp.h

clean:
	rm -rf sinchronia.capnp.*
	rm -rf sinchronia-scheduler
	rm -rf *.dSYM
