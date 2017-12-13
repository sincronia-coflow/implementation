#ifndef SINCHRONIA_RPC_H
#define SINCHRONIA_RPC_H

#include <iostream>
#include <map>
#include <vector>
#include <time.h>

#include "sinchronia.capnp.h"
#include <capnp/ez-rpc.h>
#include <kj/debug.h>

#include "coflow.hpp"
#include "scheduler.hpp"

struct coflowSchedule {
    uint32_t job_id;
    uint32_t priority;
    std::vector<data> receiving;
};

class SchedulerImpl final : public Scheduler::Server {
public:
    SchedulerImpl(
        std::map<uint32_t, coflow*> *reg, 
        std::map<uint32_t, coflow*> *rdy 
    ) : registered(reg), ready(rdy) {
        std::cout <<
            "init SchedulerImpl" <<
            std::endl;
    };

    void dumpCoflow(struct coflow *cf) {
        std::cout 
            << "{coflow " << std::endl
            << "pending: [";
        for (auto it = cf->pending_flows->begin(); it != cf->pending_flows->end(); it++) {
            std::cout 
                << "(dataId: " << it->first 
                << ", flow: <" << it->second.from << ", " << it->second.to << ", " << it->second.info.data_id << ">" 
                << "), ";
        }

        std::cout << " ]\nready: [";
        for (auto it = cf->ready_flows->begin(); it != cf->ready_flows->end(); it++) {
            std::cout 
                << "(dataId: " << it->first 
                << ", flow: <" << it->second.from << ", " << it->second.to << ", " << it->second.info.data_id << ">" 
                << "), ";
        }
        std::cout << " ]\n}" << std::endl;
    }

    void dumpState() {
        std::cout << "[registered coflows]" << std::endl;
        for (auto it = this->registered->begin(); it != this->registered->end(); it++) {
            std::cout << "jobId: " << it->first << std::endl;
            dumpCoflow(it->second);
        }

        std::cout << "[ready coflows] " << std::endl;
        for (auto it = this->ready->begin(); it != this->ready->end(); it++) {
            std::cout << "jobId: " << it->first << std::endl;
            dumpCoflow(it->second);
        }
    };

    // Initial coflow registration
    kj::Promise<void> regCoflow(RegCoflowContext context) {
        auto cfs = context.getParams().getCoflows(); // capnp::List<struct Coflow>::Reader

        for (auto it = cfs.begin(); it != cfs.end(); it++) {
            auto _fs = it->getFlows();
            auto fs = new std::map<uint32_t, flow>();
            for (auto fit = _fs.begin(); fit != _fs.end(); fit++) {
                auto f = flow{
                    .from = fit->getFrom(),
                    .to = fit->getTo(),
                    .info = {
                        .data_id = fit->getData().getDataID(),
                        .size = fit->getData().getSize(),
                    },
                };

                fs->insert(std::pair<uint32_t, flow>(f.info.data_id, f));
            }

            auto waitPair = kj::heap(kj::newPromiseAndFulfiller<uint32_t>());
            time_t now = time(NULL);
            auto cf = new coflow{
                .job_id = it->getJobID(),
                .start = now,
                .pending_flows = fs,
                .ready_flows =  new std::map<uint32_t, flow>(),
                .scheduled = kj::mv(waitPair->fulfiller),
                .uponScheduled = waitPair->promise.fork(), // for when the coflow has been scheduled
            };

            this->registered->insert(std::pair<uint32_t, coflow*>(cf->job_id, cf));
        }
        
        std::cout << "\nregCoflow()" << std::endl;
        dumpState();

        return kj::READY_NOW;
    };

    // Coflow slice from a host is ready to send
    kj::Promise<void> sendCoflow(SendCoflowContext context) {
        auto cfs = context.getParams().getCoflowSlice();
        auto job_id = cfs.getJobID();
            
        // look up against registered coflows
        auto cf_pair = this->registered->find(job_id);
        if (cf_pair == this->registered->end()) {
            // unknown coflow?
            std::cerr << "Unknown coflow " << job_id << std::endl;
            return kj::READY_NOW;
        }

        auto cf = cf_pair->second;

        auto node_id = cfs.getNodeID();
        auto snds = cfs.getSending();
        for (auto it = snds.begin(); it != snds.end(); it++) {
            data s = data {
                .data_id = it->getDataID(),
                .size = it->getSize(),
            };

            auto f_pair = cf->pending_flows->find(s.data_id);
            if (f_pair == cf->pending_flows->end()) {
                // unknown flow in coflow?
                std::cerr 
                    << "Unknown flow " << s.data_id 
                    << " in coflow " << job_id 
                    << std::endl;
                return kj::READY_NOW;
            }

            auto f = f_pair->second;

            if (f.from != node_id) {
                // unknown flow in coflow?
                std::cerr 
                    << "Inconsistent flow " << s.data_id 
                    << " in coflow " << job_id 
                    << ": sender " << node_id
                    << " != " << f.to
                    << std::endl;
                return kj::READY_NOW;
            }

            // data size is now known
            f.info.size = s.size;

            // flow is no longer pending
            cf->ready_flows->insert(std::pair<uint32_t, flow>(f.info.data_id, f));
            cf->pending_flows->erase(f_pair);
        }

        if (cf->pending_flows->empty()) {
            time_t now = time(NULL);
            cf->start = now;
            this->ready->insert(std::pair<uint32_t, coflow*>(job_id, cf));
            this->registered->erase(cf_pair);
        }

        std::cout << "\nsendCoflow(job_id " << job_id << ")\n";
        dumpState();

        std::cout << std::endl;
        return kj::READY_NOW;
    };

    // Coflow slice has nothing to do now but wait to be scheduled
    // This will block until the coflow is scheduled
    kj::Promise<void> getSchedule(GetScheduleContext context) {
        auto job_id = context.getParams().getJobId();
        auto node_id = context.getParams().getNodeId();
       
        // look up against registered coflows
        auto cf_pair = this->ready->find(job_id);
        KJ_ASSERT(cf_pair != this->ready->end());

        auto cf = cf_pair->second;
        std::cout 
            << "[getSchedule] node_id: " << node_id 
            << ", job_id: " << job_id 
            << ", promise: " << cf->scheduled->isWaiting()
            << std::endl;

        // wait for this coflow to get scheduled.
        return cf->uponScheduled
            .addBranch()
            .then([this, KJ_CPCAP(job_id), KJ_CPCAP(node_id), KJ_CPCAP(cf), KJ_CPCAP(context)](uint32_t prio) mutable {
            // Given the schedule, respond to the node
            // with a priority and which flows it needs to receive
            uint32_t priority = prio;

            std::vector<data> ret;
            for (auto it = cf->ready_flows->begin(); it != cf->ready_flows->end(); it++) {
                flow f = it->second;
                if (f.to == node_id) {
                    ret.push_back(f.info);
                }
            }

            auto result = context.getResults();
            auto cfsched = result.initSchedule();
            auto sched = cfsched.initSchedule();
            sched.setJobID(job_id);
            sched.setPriority(priority);
            
            std::cout << "[getSchedule_promise] job_id: " << job_id << ", priority: " << priority << std::endl;
            auto rec = sched.initReceiving(ret.size());
            size_t i = 0;
            for (auto it = rec.begin(); it != rec.end(); it++) {
                it->setDataID(ret[i].data_id);
                it->setSize(ret[i].size);
                i++;
            }
        });
    };

    kj::Promise<void> coflowDone(CoflowDoneContext context) {
        auto job_id = context.getParams().getJobId();
        auto node_id = context.getParams().getNodeId();
        auto finished = context.getParams().getFinished();
       
        // look up against registered coflows
        auto cf_pair = this->ready->find(job_id);
        KJ_ASSERT(cf_pair != this->ready->end());

        auto cf = cf_pair->second;
        for (auto it = finished.begin(); it != finished.end(); it++) {
            auto f_pair = cf->ready_flows->find(*it);
            KJ_ASSERT(f_pair != cf->ready_flows->end());
            KJ_ASSERT(f_pair->second.to == node_id);
            cf->ready_flows->erase(f_pair);
        }

        if (cf->ready_flows->empty()) {
            // the coflow is done.
            time_t now = time(NULL);
            auto elapsed = difftime(now, cf->start);
            std::cout 
                << "[coflowDone] "
                << "job_id: " << job_id << " "
                << "elapased: " << elapsed << " "
                << std::endl;
            this->ready->erase(cf_pair);
        }

        return kj::READY_NOW;
    };

private:
    std::map<uint32_t, coflow*> *registered;
    std::map<uint32_t, coflow*> *ready;
};
        
#endif
