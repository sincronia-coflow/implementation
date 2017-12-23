#ifndef SINCHRONIA_SCHEDULER_H
#define SINCHRONIA_SCHEDULER_H

#include <iostream>
#include <map>
#include <vector>
#include <ctime>

#include <kj/time.h>
#include "coflow.hpp"

// ticks every second
struct coflowTimeUnit{ 
    time_t epoch_start;
    uint32_t timeStep;
}; // wall clock seconds since epoch start

struct coflowTimeUnit sinchronia_epoch_reset() {
    return {
        .epoch_start = time(NULL),
        .timeStep = 0,
    };
}

void sinchronia_update_time(struct coflowTimeUnit *now) {
    time_t wall = time(NULL);
    double seconds = difftime(wall, now->epoch_start);
    now->timeStep = (uint32_t) seconds;
};

kj::Duration sinchronia_duration(uint32_t time_steps) {
    return time_steps * kj::SECONDS;
}

class CoflowScheduler {
public:
    coflowTimeUnit current_time;

    CoflowScheduler() {
        current_time = sinchronia_epoch_reset();
    };

    // Decide whether to run the scheduler now
    virtual kj::Duration time_to_schedule() = 0;

    // Schedule the coflows by shuffling the order within the vector.
    virtual void schedule(std::vector<coflow*> *to_schedule) = 0;
};

class DummyScheduler : public CoflowScheduler {
public:
    coflowTimeUnit next_phase_time;
    DummyScheduler() : CoflowScheduler() {
        this->next_phase_time = this->current_time;
    };

    // The dummy scheduler runs every 5 seconds
    virtual kj::Duration time_to_schedule() {
        this->next_phase_time = this->current_time;
        this->next_phase_time.timeStep += 5;
        return sinchronia_duration(5);
    };

    // The dummy scheduler picks an arbitrary order - the one it was handed.
    virtual void schedule(std::vector<coflow*> *to_schedule) {
        sinchronia_update_time(&current_time);
        if (current_time.timeStep < next_phase_time.timeStep) {
            std::cout
                << "[scheduler] DummyScheduler, work conservation schedule: "
                << "not scheduling " << to_schedule->size() << " coflows\n";
        } else {
            std::cout
                << "[scheduler] DummyScheduler, phased schedule: "
                << "not scheduling " << to_schedule->size() << " coflows\n";
        }

        return;
    };
};

#endif
