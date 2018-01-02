#ifndef SINCHRONIA_COFLOW_H
#define SINCHRONIA_COFLOW_H

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

#endif
