#ifndef SINCHRONIA_SCHEDULER_H
#define SINCHRONIA_SCHEDULER_H

#include <iostream>
#include <map>
#include <vector>
#include <ctime>

#include <kj/time.h>
#include "coflow.hpp"

class CoflowScheduler {
public:
    CoflowScheduler() {};

    // Decide whether to run the scheduler now
    virtual kj::Duration time_to_schedule() = 0;

    // Schedule the coflows by shuffling the order within the vector.
    virtual void schedule(std::vector<coflow*> *to_schedule) = 0;
};

class DummyScheduler : public CoflowScheduler {
public:
    DummyScheduler() : CoflowScheduler() {};

    // The dummy scheduler runs every 5 seconds
    virtual kj::Duration time_to_schedule() {
        std::cout
            << "DummyScheduler "
            << "waiting 5 seconds\n";
        return 5 * kj::SECONDS;
    };

    // The dummy scheduler picks an arbitrary order - the one it was handed.
    virtual void schedule(std::vector<coflow*> *to_schedule) {
        std::cout
            << "DummyScheduler "
            << "not scheduling " << to_schedule->size() << " coflows\n";
        return;
    };
};

#endif
