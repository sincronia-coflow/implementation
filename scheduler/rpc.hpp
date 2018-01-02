#ifndef SINCHRONIA_RPC_H
#define SINCHRONIA_RPC_H

#include <iostream>
#include <map>
#include <vector>
#include <time.h>

#include "sinchronia.capnp.h"
#include <capnp/ez-rpc.h>
#include <kj/async.h>
#include <kj/async-io.h>
#include <kj/memory.h>
#include <kj/debug.h>

#include "coflow.hpp"
#include "scheduler.hpp"

class RpcHandler final : kj::TaskSet::ErrorHandler {
public:
    RpcHandler(CoflowScheduler *sch);

    void taskFailed(kj::Exception &&exception) override;
    void do_schedule();
    void start_scheduler_timer();
    void start_rpc_handler();

    kj::AsyncIoContext ioContext;
    kj::TaskSet tasks;

    std::map<uint32_t, coflow*> *registered;
    std::map<uint32_t, coflow*> *ready;
    std::vector<coflow*> *schedule;
    CoflowScheduler *cf_sch;
};

struct coflowSchedule {
    uint32_t job_id;
    uint32_t priority;
    std::vector<data> receiving;
};

class SchedulerImpl final : public Scheduler::Server {
public:
    SchedulerImpl(RpcHandler *rpc) : registered(rpc->registered), ready(rpc->ready), rpch(rpc) {
        std::cout 
            << "init SchedulerImpl" 
            << std::endl;
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
                << ", flow: <" << it->second.from << " -> " << it->second.to << ": " << it->second.info.data_id << ">" 
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
            std::cout 
                << "jobId: " << it->first 
                << ", priority: " << it->second->priority
                << std::endl;
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

            auto waitPair = kj::heap(kj::newPromiseAndFulfiller<void>());
            time_t now = time(NULL);
            auto cf = new coflow{
                .job_id = it->getJobID(),
                .priority = 0x7fffff,
                .wall_start = now,
                .bottleneck_size = 0,
                .pending_flows = fs,
                .ready_flows =  new std::map<uint32_t, flow>(),
                .ready = kj::mv(waitPair->fulfiller),
                .uponReady = waitPair->promise.fork(), // when the coflow has all flows accounted for
            };

            cf->bottleneck_size = get_bottleneck_size(cf);
            this->registered->insert(std::pair<uint32_t, coflow*>(cf->job_id, cf));
        }
        
        std::cout << "\nregCoflow()" << std::endl;
        dumpState();

        return kj::READY_NOW;
    };

    // Coflow slice from a host is ready to send
    kj::Promise<void> sendCoflow(SendCoflowContext context) {
        coflow *cf;
        auto cfs = context.getParams();
        auto node_id = cfs.getNodeID();
        auto job_id = cfs.getJobID();
        std::cout << "\nsendCoflow(job_id " << job_id << ")\n";
        dumpState();
        std::cout << std::endl;
            
        // look up against registered coflows
        auto cf_pair = this->registered->find(job_id);
        if (cf_pair != this->registered->end()) {
            cf = cf_pair->second;

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
                cf->wall_start = now;
                this->ready->insert(std::pair<uint32_t, coflow*>(job_id, cf));
                this->registered->erase(cf_pair);
                cf->ready->fulfill();

                this->rpch->do_schedule();
            }
        } else {
            cf_pair = this->ready->find(job_id);
            if (cf_pair == this->ready->end()) {
                std::cout
                    << "Unknown coflow " << job_id << std::endl;
                return kj::READY_NOW;
            }

            cf = cf_pair->second;
        }

        return cf->uponReady
            .addBranch()
            .then([this, KJ_CPCAP(node_id), KJ_CPCAP(cf), KJ_CPCAP(context)]() mutable {
        
             std::vector<data> ret;
             for (auto it = cf->ready_flows->begin(); it != cf->ready_flows->end(); it++) {
                 flow f = it->second;
                 if (f.to == node_id) {
                     ret.push_back(f.info);
                 }
             }

             auto results = context.getResults();
             auto recvs = results.initReceiving(ret.size());
             size_t i = 0;
             for (auto it = recvs.begin(); it != recvs.end(); it++) {
                 it->setDataID(ret[i].data_id);
                 it->setSize(ret[i].size);
                 i++;
             }
        });
    };

    // Which coflows have been assigned a priority for this node?
    // Clients poll each time a new flow is sent to allow reprioritization
    kj::Promise<void> getSchedule(GetScheduleContext context) {
        size_t i;
        auto node_id = context.getParams().getNodeId();
        auto cfs_at_node = new std::vector<coflow*>();

        // which coflows contain flows for which this node is the sender?
        for (auto cf_it = this->ready->begin(); cf_it != this->ready->end(); cf_it++) {
            for (auto f_it = cf_it->second->ready_flows->begin(); f_it != cf_it->second->ready_flows->end(); f_it++) {
                if (f_it->second.from == node_id) {
                    cfs_at_node->push_back(cf_it->second);
                    break;
                }
            }
        }

        if (cfs_at_node->empty()) {
            // no coflows for this node. Return empty list.
            //std::cout 
            //    << "[getSchedule] "
            //    << "node_id: " << node_id  
            //    << " no coflows"
            //    << std::endl;
            return kj::READY_NOW;
        }
        
        //std::cout << "[getSchedule] node_id: " << node_id  << "[ ";
        //for (auto it = cfs_at_node->begin(); it != cfs_at_node->end(); it++) {
        //    std::cout
        //        << "(cf: " << (*it)->job_id 
        //        << ", prio: " << (*it)->priority << ") ";
        //}
        //std::cout << "]" << std::endl;

        auto result = context.getResults();
        auto sched = result.initSchedule(cfs_at_node->size());
        i = 0;
        for (auto it = sched.begin(); it != sched.end(); it++) {
            it->setJobID(cfs_at_node->at(i)->job_id);
            it->setPriority(cfs_at_node->at(i)->priority);
            i++;
        }

        return kj::READY_NOW;
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
            auto elapsed = difftime(now, cf->wall_start);
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
    RpcHandler *rpch;
};
        
#endif
