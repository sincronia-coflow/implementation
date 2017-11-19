#include <assert.h>
#include <iostream>
#include <shared_mutex>
#include <vector>
#include <map>

#include "sinchronia.capnp.h"
#include <capnp/rpc-twoparty.h>
#include <capnp/message.h>
#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/memory.h>

#include "scheduler.hpp"
#include "rpc.hpp"

class RpcHandler final : kj::TaskSet::ErrorHandler {
public:
    RpcHandler(CoflowScheduler *sch) : 
        ioContext(kj::setupAsyncIo()), 
        tasks(*this),
        registered(new std::map<uint32_t, coflow*>),
        ready(new std::map<uint32_t, coflow*>),
        schedule(new std::vector<coflow*>),
        cf_sch(sch)
    {};

    void taskFailed(kj::Exception &&exception) override {
        std::cerr << 
            "exception: " <<
            exception.getDescription().cStr() <<
            std::endl; 
        kj::throwFatalException(kj::mv(exception));
    };

    void do_schedule() {
        // clear schedule vector
        this->schedule->clear();
        // all ready coflows appended to schedule vector
        for (auto it = this->ready->begin(); it != this->ready->end(); it++) {
            this->schedule->push_back(it->second);
        }

        // Schedule coflows using templated scheduling algorithm implementation
        this->cf_sch->schedule(this->schedule);
        
        // if the scheduling promise is not yet fulfilled, fulfill it
        for (auto it = this->schedule->begin(); it != this->schedule->end(); it++) {
            coflow *curr_cf = *it;
            if ((curr_cf->uponScheduled).fulfiller->isWaiting()) {
                (curr_cf->uponScheduled).fulfiller->fulfill();
            }
        }
    };

    void start_scheduler_timer() {
        kj::Duration dur = this->cf_sch->time_to_schedule();
        this->tasks.add(this->ioContext.provider->getTimer().afterDelay(dur).then([this]() {
            this->start_scheduler_timer();
            this->do_schedule();
        }));
    };

    void start_rpc_handler() {
        std::cout << "init\n";

        std::cout << "init impl...";
        capnp::Capability::Client impl = kj::heap<SchedulerImpl>(
            this->registered, 
            this->ready, 
            this->schedule
        ); 

        capnp::TwoPartyServer server(impl);
        std::cout << "done\n";

        std::cout << "init server...";
        auto& waitScope = this->ioContext.waitScope;
        auto addr = this->ioContext.provider->getNetwork().parseAddress("*", 16424).wait(waitScope);
        auto listener = addr->listen();
        std::cout << "done\n";

        auto port = listener->getPort();
        std::cout << "listening on port " << port << std::endl;

        std::cout << "waiting...\n";
        start_scheduler_timer();
        this->tasks.add(server.listen(*listener));

        kj::NEVER_DONE.wait(waitScope);
    };

    kj::AsyncIoContext ioContext;
    kj::TaskSet tasks;

    std::map<uint32_t, coflow*> *registered;
    std::map<uint32_t, coflow*> *ready;
    std::vector<coflow*> *schedule;
    CoflowScheduler *cf_sch;
};

int main(int argv, char **argc) {
    CoflowScheduler *sch = new DummyScheduler();
    RpcHandler handler(sch);
    handler.start_rpc_handler();
    return 0;
}
