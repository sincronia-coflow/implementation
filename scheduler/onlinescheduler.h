#ifndef ONLINESCHEDULER
#define ONLINESCHEDULER

#include "coflow.h"
#include "common.h"
#include <vector>

std::vector<Coflow> generate_online_admissible_set_coflows(std::vector<Coflow> unscheduled_coflows,double deadline);
std::vector<Coflow> find_candidate_coflow_set(std::vector<Coflow> C, double deadline);

#endif
