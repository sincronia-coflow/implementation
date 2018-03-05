#ifndef SINCRONIA_COFLOW_H
#define SINCRONIA_COFLOW_H

#include <iostream>
#include <map>
#include <vector>

#include <kj/async.h>
#include <time.h>

struct data {
    uint32_t data_id;
    uint32_t size;
};

struct flow {
    uint32_t from;
    uint32_t to;
    data info;
};

struct coflow {
    uint32_t job_id;
    uint32_t priority;
    time_t wall_start;
    uint32_t bottleneck_size;
    std::map<uint32_t, flow> *pending_flows;
    std::map<uint32_t, flow> *ready_flows;
    kj::Own<kj::PromiseFulfiller<void>> ready;
    kj::ForkedPromise<void> uponReady;
};

uint32_t get_bottleneck_size(struct coflow *cf) {
    auto senders = new std::map<uint32_t, uint32_t>();
    auto receivers = new std::map<uint32_t, uint32_t>();

    for (auto it = cf->pending_flows->begin(); it != cf->pending_flows->end(); it++) {
        flow f = it->second;
        auto senders_it = senders->find(f.from);
        if (senders_it == senders->end()) {
            senders->insert(std::pair<uint32_t, uint32_t>(f.from, f.info.size));
        } else {
            senders_it->second += f.info.size;
        }
        
        auto receivers_it = receivers->find(f.to);
        if (receivers_it == receivers->end()) {
            receivers->insert(std::pair<uint32_t, uint32_t>(f.to, f.info.size));
        } else {
            receivers_it->second += f.info.size;
        }
    }

    uint32_t bottleneck_size = 0;
    for (auto it = senders->begin(); it != senders->end(); it++) {
        if (it->second > bottleneck_size) {
            bottleneck_size = it->second;
        }
    }
    
    for (auto it = receivers->begin(); it != receivers->end(); it++) {
        if (it->second > bottleneck_size) {
            bottleneck_size = it->second;
        }
    }

    return bottleneck_size;
};

#endif
