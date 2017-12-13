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
    time_t start;
    std::map<uint32_t, flow> *pending_flows;
    std::map<uint32_t, flow> *ready_flows;
    kj::Own<kj::PromiseFulfiller<uint32_t>> scheduled;
    kj::ForkedPromise<uint32_t> uponScheduled;
};

#endif
