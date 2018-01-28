#ifndef COMMON_H
#define COMMON_H

#include "coflow.h"
#include "bigswitch.h"
#include <vector>
#include <fstream>

extern Bigswitch B;
extern std::vector<Coflow> inadmissible_coflows;
extern std::ofstream myfile;
extern std::ofstream logfile;
extern std::ofstream logfile2;

#endif
