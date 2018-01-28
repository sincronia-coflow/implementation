#ifndef COMMON_H
#define COMMON_H

#include "coflow.h"
#include "bigswitch.h"
// #include "ports.h"
#include <vector>
#include <fstream>

// extern std::vector<Coflow> vector_coflows;
extern Bigswitch B;
// extern std::vector<Sender> S;
// extern std::vector<Receiver> R;
// extern double counter;
// extern std::vector<Coflow> work_conservation_coflows;
extern std::vector<Coflow> inadmissible_coflows;
extern std::ofstream myfile;
extern std::ofstream logfile;
extern std::ofstream logfile2;

#endif
