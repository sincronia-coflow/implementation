#include <assert.h>
#include <iostream>
#include <vector>
#include <map>
#include <ctime>
#include <algorithm>
#include "sincronia.capnp.h"
#include <algorithm>
#include <capnp/rpc-twoparty.h>
#include <capnp/message.h>
#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/memory.h>

#include "scheduler.hpp"
#include "rpc.hpp"
std::ofstream myfile;
  //  myfile.open ("coflow-done.txt");
RpcHandler::RpcHandler(CoflowScheduler *sch) :
    ioContext(kj::setupAsyncIo()),
    tasks(*this),
    registered(new std::map<uint32_t, coflow*>),
    ready(new std::map<uint32_t, coflow*>),
    schedule(new std::vector<coflow*>),
    cf_sch(sch)
{};

void RpcHandler::taskFailed(kj::Exception &&exception) {
    std::cerr <<
        "exception: " <<
        exception.getDescription().cStr() <<
        std::endl;
    kj::throwFatalException(kj::mv(exception));
};

void RpcHandler::do_schedule() {
    // clear schedule vector
    this->schedule->clear();
    // all ready coflows appended to schedule vector
    for (auto it = this->ready->begin(); it != this->ready->end(); it++) {
        this->schedule->push_back(it->second);
    }

    // Schedule coflows using templated scheduling algorithm implementation
    this->cf_sch->schedule(this->schedule);

    // if the scheduling promise is not yet fulfilled, fulfill it
    uint32_t prio = 0;
    for (auto it = this->schedule->begin(); it != this->schedule->end(); it++) {
        coflow *curr_cf = *it;
        curr_cf->priority = std::max(7-((int)prio),0);
        std::cout
            << "[scheduler] fulfilling cf "
            << curr_cf->job_id << ": "
            << curr_cf->priority
            << std::endl;
        prio++;
    }
};

void RpcHandler::start_scheduler_timer() {
    kj::Duration dur = this->cf_sch->time_to_schedule();
    std::cout << "[scheduler] waiting\n";
    this->tasks.add(this->ioContext.provider->getTimer().afterDelay(dur).then([this]() {
                this->start_scheduler_timer();
                std::cout << "[scheduler] scheduling NOW\n";
                this->do_schedule();
                }));
};

void RpcHandler::start_rpc_handler() {
    capnp::Capability::Client impl = kj::heap<SchedulerImpl>(this);

    capnp::TwoPartyServer server(impl);

    auto& waitScope = this->ioContext.waitScope;
    auto addr = this->ioContext.provider->getNetwork().parseAddress("*", 16424).wait(waitScope);
    auto listener = addr->listen();

    auto port = listener->getPort();
    std::cout << "[rpc] listening on port " << port << std::endl;

    start_scheduler_timer();
    std::cout << "[rpc] waiting...\n";
    this->tasks.add(server.listen(*listener));

    kj::NEVER_DONE.wait(waitScope);
};

int main(int argv, char **argc) {
    myfile.open ("coflow-done.txt");    
// CoflowScheduler *sch = new DummyScheduler();
    std::cout << "Time of calling the scheduler: " << time(NULL) << '\n';
    CoflowScheduler *sch = new OnlineScheduler();
    RpcHandler handler(sch);
    handler.start_rpc_handler();
    return 0;
}
