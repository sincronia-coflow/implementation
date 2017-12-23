all: scheduler

scheduler: sincronia.capnp.h main.cpp
	g++ -o sincronia-scheduler -std=c++14 sincronia.capnp.c++ main.cpp coflow.cpp flow.cpp onlinescheduler.cpp primal_dual.cpp -lcapnp -lcapnp-rpc -lkj -lkj-async -g -gdwarf-2 -L/Library/gurobi702/mac64/lib/ -lgurobi_c++ -lgurobi70

sincronia.capnp.h:
	capnp compile -I${GOPATH}/src/zombiezen.com/go/capnproto2/std -oc++ --src-prefix=.. ../sincronia.capnp

capnp: sincronia.capnp.h

clean:
	rm -rf sincronia.capnp.*
	rm -rf sincronia-scheduler
	rm -rf *.dSYM
