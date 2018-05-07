all: scheduler

scheduler: sincronia.capnp.h main.cpp
	g++ -o sincronia-scheduler -std=c++14 sincronia.capnp.c++ main.cpp -lcapnp -lcapnp-rpc -lkj -lkj-async -g -gdwarf-2

sincronia.capnp.h:
	capnp compile -I${GOPATH}/src/zombiezen.com/go/capnproto2/std -oc++ --src-prefix=.. ../sincronia.capnp

capnp: sincronia.capnp.h

clean:
	rm -rf sincronia.capnp.*
	rm -rf sincronia-scheduler
	rm -rf *.dSYM
